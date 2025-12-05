(* Local TCP server for relay clients *)

(* Client request message *)
type client_request = {
  action : string;
  symbols : string list;
}
[@@deriving yojson]

(* Client connection *)
type client = {
  id : int;
  flow : Eio.Flow.two_way_ty Eio.Resource.t;
  mutable subscribed_symbols : string list;
}

(* Connected clients *)
let clients : client list ref = ref []
let next_client_id = ref 0
let clients_mutex = Eio.Mutex.create ()

(* Add a client *)
let add_client flow =
  Eio.Mutex.use_rw clients_mutex ~protect:true (fun () ->
    let id = !next_client_id in
    incr next_client_id;
    let client = { id; flow; subscribed_symbols = [] } in
    clients := client :: !clients;
    Eio.traceln "Local: Client %d connected (total: %d)" id (List.length !clients);
    client
  )

(* Remove a client *)
let remove_client client =
  Eio.Mutex.use_rw clients_mutex ~protect:true (fun () ->
    clients := List.filter (fun c -> c.id <> client.id) !clients;
    Eio.traceln "Local: Client %d disconnected (total: %d)" client.id (List.length !clients)
  )

(* Get all unique subscribed symbols across all clients *)
let all_subscribed_symbols () =
  Eio.Mutex.use_ro clients_mutex (fun () ->
    !clients
    |> List.concat_map (fun c -> c.subscribed_symbols)
    |> List.sort_uniq ~cmp:Stdlib.compare
  )

(* Update client subscriptions *)
let update_subscriptions client symbols =
  Eio.Mutex.use_rw clients_mutex ~protect:true (fun () ->
    client.subscribed_symbols <- List.sort_uniq ~cmp:Stdlib.compare
      (client.subscribed_symbols @ symbols);
    Eio.traceln "Local: Client %d now subscribed to %d symbols"
      client.id (List.length client.subscribed_symbols)
  )

(* Send message to a client *)
let send_to_client client msg =
  try
    let msg_with_newline = msg ^ "\n" in
    Eio.Flow.copy_string msg_with_newline client.flow;
    true
  with _ ->
    false

(* Broadcast message to clients subscribed to a symbol *)
let broadcast_for_symbol symbol msg =
  Eio.Mutex.use_ro clients_mutex (fun () ->
    !clients |> List.iter (fun client ->
      if List.mem symbol client.subscribed_symbols then
        ignore (send_to_client client msg)
    )
  )

(* Broadcast raw JSON message, checking symbol in the message *)
let broadcast_aggregate json_str =
  (* Parse to get symbol, then broadcast *)
  try
    let json = Yojson.Safe.from_string json_str in
    let symbol = Yojson.Safe.Util.(member "sym" json |> to_string) in
    broadcast_for_symbol symbol json_str
  with _ ->
    (* If parsing fails, broadcast to all *)
    Eio.Mutex.use_ro clients_mutex (fun () ->
      !clients |> List.iter (fun client ->
        ignore (send_to_client client json_str)
      )
    )

(* Read a line from client *)
let read_line flow buf =
  try
    let rec read_until_newline () =
      let chunk = Cstruct.create 1024 in
      let n = Eio.Flow.single_read flow chunk in
      Buffer.add_string buf (Cstruct.to_string ~len:n chunk);
      let contents = Buffer.contents buf in
      match String.index_opt contents '\n' with
      | Some i ->
        let line = String.sub contents 0 i in
        let rest = String.sub contents (i + 1) (String.length contents - i - 1) in
        Buffer.clear buf;
        Buffer.add_string buf rest;
        Some line
      | None ->
        read_until_newline ()
    in
    read_until_newline ()
  with
  | End_of_file -> None
  | _ -> None

(* Handle a client connection *)
let handle_client ~on_subscribe flow _addr =
  let client = add_client flow in
  let buf = Buffer.create 256 in

  (* Send welcome message *)
  let welcome = `Assoc [("status", `String "connected"); ("client_id", `Int client.id)] in
  ignore (send_to_client client (Yojson.Safe.to_string welcome));

  let rec loop () =
    match read_line flow buf with
    | None ->
      remove_client client
    | Some line ->
      (try
        let json = Yojson.Safe.from_string line in
        let req = client_request_of_yojson json in
        match req.action with
        | "subscribe" ->
          update_subscriptions client req.symbols;
          on_subscribe req.symbols;
          let resp = `Assoc [
            ("status", `String "subscribed");
            ("symbols", `List (List.map (fun s -> `String s) client.subscribed_symbols))
          ] in
          ignore (send_to_client client (Yojson.Safe.to_string resp))
        | "unsubscribe" ->
          Eio.Mutex.use_rw clients_mutex ~protect:true (fun () ->
            client.subscribed_symbols <- List.filter
              (fun s -> not (List.mem s req.symbols))
              client.subscribed_symbols
          );
          let resp = `Assoc [("status", `String "unsubscribed")] in
          ignore (send_to_client client (Yojson.Safe.to_string resp))
        | _ ->
          let resp = `Assoc [("error", `String "unknown action")] in
          ignore (send_to_client client (Yojson.Safe.to_string resp))
      with e ->
        Eio.traceln "Local: Error parsing client message: %s" (Printexc.to_string e);
        let resp = `Assoc [("error", `String "invalid json")] in
        ignore (send_to_client client (Yojson.Safe.to_string resp)));
      loop ()
  in
  loop ()

(* Start the local TCP server *)
let start ~sw ~env ~port ~on_subscribe =
  let net = Eio.Stdenv.net env in
  let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, port) in

  Eio.traceln "Local: Starting server on port %d" port;

  let socket = Eio.Net.listen net ~sw ~backlog:10 ~reuse_addr:true addr in

  Eio.Fiber.fork ~sw (fun () ->
    while true do
      Eio.Net.accept_fork socket ~sw (fun flow addr ->
        handle_client ~on_subscribe (flow :> Eio.Flow.two_way_ty Eio.Resource.t) addr
      )
        ~on_error:(fun e ->
          Eio.traceln "Local: Client error: %s" (Printexc.to_string e)
        )
    done
  )

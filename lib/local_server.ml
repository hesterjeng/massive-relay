(* Local WebSocket server for relay clients *)

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

(* Update client subscriptions *)
let update_subscriptions client symbols =
  Eio.Mutex.use_rw clients_mutex ~protect:true (fun () ->
    client.subscribed_symbols <- List.sort_uniq ~cmp:Stdlib.compare
      (client.subscribed_symbols @ symbols);
    Eio.traceln "Local: Client %d now subscribed to %d symbols"
      client.id (List.length client.subscribed_symbols)
  )

(* Send WebSocket text frame to a client (server frames are NOT masked) *)
let send_to_client client msg =
  try
    let frame = Websocket.Frame.{
      fin = true;
      opcode = Text;
      mask = false;  (* Server frames are not masked *)
      payload = msg;
    } in
    let encoded = Websocket.Frame.encode frame in
    Eio.Flow.copy_string encoded client.flow;
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

(* Compute SHA-1 hash for WebSocket accept key *)
let sha1_hash str =
  (* Simple SHA-1 implementation for WebSocket handshake *)
  (* We use Mirage_crypto for this *)
  let open Mirage_crypto.Hash in
  let digest = SHA1.digest (Cstruct.of_string str) in
  Cstruct.to_string digest

(* WebSocket server handshake *)
let websocket_handshake flow =
  (* Read HTTP request *)
  let buf = Buffer.create 1024 in
  let rec read_until_double_crlf () =
    let chunk = Cstruct.create 1024 in
    let n = Eio.Flow.single_read flow chunk in
    Buffer.add_string buf (Cstruct.to_string ~len:n chunk);
    let contents = Buffer.contents buf in
    (* Check for \r\n\r\n *)
    let rec find_end pos =
      if pos + 3 >= String.length contents then
        read_until_double_crlf ()
      else if Char.equal (String.get contents pos) '\r' &&
              Char.equal (String.get contents (pos + 1)) '\n' &&
              Char.equal (String.get contents (pos + 2)) '\r' &&
              Char.equal (String.get contents (pos + 3)) '\n' then
        contents
      else
        find_end (pos + 1)
    in
    find_end 0
  in
  let request = read_until_double_crlf () in

  (* Extract Sec-WebSocket-Key header *)
  let lines = String.split_on_char '\n' request in
  let ws_key = ref None in
  List.iter (fun line ->
    let line = String.trim line in
    if String.length line > 19 &&
       String.equal (String.lowercase_ascii (String.sub line 0 18)) "sec-websocket-key:" then
      ws_key := Some (String.trim (String.sub line 18 (String.length line - 18)))
  ) lines;

  match !ws_key with
  | None ->
    Eio.traceln "Local: No Sec-WebSocket-Key in request";
    Error "No WebSocket key"
  | Some key ->
    (* Compute accept key: SHA1(key + magic) then base64 *)
    let magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11" in
    let accept_raw = sha1_hash (key ^ magic) in
    let accept_key = Base64.encode_string accept_raw in

    (* Send HTTP 101 response *)
    let response = Printf.sprintf
      "HTTP/1.1 101 Switching Protocols\r\n\
       Upgrade: websocket\r\n\
       Connection: Upgrade\r\n\
       Sec-WebSocket-Accept: %s\r\n\
       \r\n"
      accept_key
    in
    Eio.Flow.copy_string response flow;
    Eio.traceln "Local: WebSocket handshake completed";
    Ok ()

(* Receive a WebSocket frame from client *)
let receive_frame flow =
  Websocket.Frame.decode flow

(* Handle a client connection *)
let handle_client ~on_subscribe flow _addr =
  (* Perform WebSocket handshake *)
  match websocket_handshake flow with
  | Error _ -> ()
  | Ok () ->
    let client = add_client flow in

    (* Send welcome message *)
    let welcome = `Assoc [("status", `String "connected"); ("client_id", `Int client.id)] in
    ignore (send_to_client client (Yojson.Safe.to_string welcome));

    let rec loop () =
      match receive_frame flow with
      | Error `ConnectionClosed ->
        remove_client client
      | Error _ ->
        remove_client client
      | Ok frame ->
        match frame.Websocket.Frame.opcode with
        | Text ->
          let payload = frame.payload in
          (try
            let json = Yojson.Safe.from_string payload in
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
        | Ping ->
          (* Respond with pong *)
          let pong = Websocket.Frame.{
            fin = true;
            opcode = Pong;
            mask = false;
            payload = frame.payload;
          } in
          let encoded = Websocket.Frame.encode pong in
          Eio.Flow.copy_string encoded flow;
          loop ()
        | Close ->
          (* Send close frame back *)
          let close = Websocket.Frame.{
            fin = true;
            opcode = Close;
            mask = false;
            payload = "";
          } in
          let encoded = Websocket.Frame.encode close in
          (try Eio.Flow.copy_string encoded flow with _ -> ());
          remove_client client
        | _ ->
          loop ()
    in
    loop ()

(* Start the local WebSocket server *)
let start ~sw ~env ~port ~on_subscribe =
  let net = Eio.Stdenv.net env in
  let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, port) in

  Eio.traceln "Local: Starting WebSocket server on ws://localhost:%d" port;

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

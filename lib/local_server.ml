(* Local WebSocket server for relay clients *)
(* Speaks Massive protocol for drop-in compatibility *)

(* Massive-style request message: {"action":"subscribe","params":"A.AAPL,Q.AAPL,A.MSFT"} *)
type client_request = {
  action : string;
  params : string;
}
[@@deriving yojson]

(* Parse Massive params string into channel list *)
(* "A.AAPL,Q.AAPL,A.MSFT" -> ["A.AAPL"; "Q.AAPL"; "A.MSFT"] *)
let parse_params params =
  String.split_on_char ',' params
  |> List.map String.trim
  |> List.filter (fun s -> String.length s > 0)

(* Build Massive-style status message array *)
let status_message status message =
  let msg = `Assoc [
    ("ev", `String "status");
    ("status", `String status);
    ("message", `String message);
  ] in
  Yojson.Safe.to_string (`List [msg])

(* Client connection. [cluster] is fixed at connect time by the request PATH
   (/stocks | /futures) and scopes both this client's subscriptions (to the matching
   upstream) and the broadcasts it receives (only that cluster's data). *)
type client = {
  id : int;
  flow : Eio.Flow.two_way_ty Eio.Resource.t;
  cluster : Cluster.t;
  mutable subscribed_symbols : string list;
}

(* Connected clients *)
let clients : client list ref = ref []
let next_client_id = ref 0
let clients_mutex = Eio.Mutex.create ()

(* Add a client on a given cluster *)
let add_client ~cluster flow =
  Eio.Mutex.use_rw clients_mutex ~protect:true (fun () ->
    let id = !next_client_id in
    incr next_client_id;
    let client = { id; flow; cluster; subscribed_symbols = [] } in
    clients := client :: !clients;
    Log.traceln "Local: Client %d connected on %s (total: %d)" id
      (Cluster.to_string cluster) (List.length !clients);
    client
  )

(* Remove a client *)
let remove_client client =
  Eio.Mutex.use_rw clients_mutex ~protect:true (fun () ->
    clients := List.filter (fun c -> c.id <> client.id) !clients;
    Log.traceln "Local: Client %d disconnected (total: %d)" client.id (List.length !clients)
  )

(* Update client subscriptions *)
let update_subscriptions client symbols =
  Eio.Mutex.use_rw clients_mutex ~protect:true (fun () ->
    client.subscribed_symbols <- List.sort_uniq ~cmp:Stdlib.compare
      (client.subscribed_symbols @ symbols);
    Log.traceln "Local: Client %d now subscribed to %d symbols"
      client.id (List.length client.subscribed_symbols)
  )

(* Send WebSocket text frame to a client (server frames are NOT masked).
   Uses a 5 second timeout to prevent a stalled client from blocking broadcast. *)
let send_to_client ~clock client msg =
  try
    let frame = Websocket.Frame.{
      fin = true;
      opcode = Text;
      mask = false;
      payload = msg;
    } in
    let encoded = Websocket.Frame.encode frame in
    Eio.Time.with_timeout_exn clock 5.0 (fun () ->
      Eio.Flow.copy_string encoded client.flow);
    true
  with exn ->
    Log.traceln "Local: Failed to send to client %d: %s" client.id (Printexc.to_string exn);
    false

(* Stats for diagnostics - reset every 60s to avoid overflow *)
let broadcast_count = ref 0
let send_success_count = ref 0
let send_failure_count = ref 0
let last_stats_time = ref 0.0

(* Log stats periodically (called inside mutex, so quick) *)
let maybe_log_stats () =
  let now = Unix.gettimeofday () in
  if Float.(now -. !last_stats_time > 60.0) then begin
    let num_clients = List.length !clients in
    let client_info = !clients |> List.map (fun c ->
      Printf.sprintf "%d:%d" c.id (List.length c.subscribed_symbols)
    ) |> String.concat "," in
    Log.traceln "Local: STATS bc=%d ok=%d fail=%d clients=%d [%s]"
      !broadcast_count !send_success_count !send_failure_count num_clients client_info;
    broadcast_count := 0;
    send_success_count := 0;
    send_failure_count := 0;
    last_stats_time := now
  end

(* Broadcast to the clients of ONE cluster (a cluster's upstream data must not leak
   to another cluster's clients). Every message is for a symbol some client of that
   cluster requested, so no per-symbol filtering is needed — clients ignore
   irrelevant symbols. Collects failed clients under read lock, removes them after. *)
let broadcast ~clock ~cluster msg =
  let failed =
    Eio.Mutex.use_ro clients_mutex (fun () ->
      incr broadcast_count;
      let failed =
        !clients
        |> List.filter (fun c -> Cluster.equal c.cluster cluster)
        |> List.filter_map (fun client ->
          if send_to_client ~clock client msg then
            (incr send_success_count; None)
          else
            (incr send_failure_count; Some client))
      in
      maybe_log_stats ();
      failed)
  in
  List.iter remove_client failed

(* Broadcast data message wrapped in array (Massive protocol) *)
let broadcast_data ~clock ~cluster json_str =
  broadcast ~clock ~cluster ("[" ^ json_str ^ "]")

(* Send a Ping frame to a cluster's clients, keeping connections alive. *)
let broadcast_ping ~clock ~cluster =
  let ping_frame = Websocket.Frame.{
    fin = true; opcode = Ping; mask = false; payload = "";
  } in
  let encoded = Websocket.Frame.encode ping_frame in
  let failed =
    Eio.Mutex.use_ro clients_mutex (fun () ->
      !clients
      |> List.filter (fun c -> Cluster.equal c.cluster cluster)
      |> List.filter_map (fun client ->
        try
          Eio.Time.with_timeout_exn clock 5.0 (fun () ->
            Eio.Flow.copy_string encoded client.flow);
          None
        with _ -> Some client))
  in
  List.iter remove_client failed

(* Compute SHA-1 hash for WebSocket accept key *)
let sha1_hash str =
  Digestif.SHA1.digest_string str |> Digestif.SHA1.to_raw_string

(* WebSocket server handshake - returns leftover bytes after HTTP headers *)
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
        (contents, pos + 4)
      else
        find_end (pos + 1)
    in
    find_end 0
  in
  let (full_data, header_end) = read_until_double_crlf () in
  let request = String.sub full_data 0 header_end in
  let leftover =
    if header_end < String.length full_data then
      String.sub full_data header_end (String.length full_data - header_end)
    else ""
  in

  (* The request line ("GET /futures HTTP/1.1") carries the PATH that selects the
     cluster. Second whitespace-separated token; default "/" if malformed. *)
  let path =
    match String.split_on_char '\n' request with
    | line :: _ -> (
      match String.split_on_char ' ' (String.trim line) with
      | _method :: p :: _ -> p
      | _ -> "/")
    | [] -> "/"
  in

  (* Extract Sec-WebSocket-Key header *)
  let lines = String.split_on_char '\n' request in
  let ws_key = List.find_map (fun line ->
    let line = String.trim line in
    if String.length line > 19 &&
       String.equal (String.lowercase_ascii (String.sub line 0 18)) "sec-websocket-key:" then
      Some (String.trim (String.sub line 18 (String.length line - 18)))
    else
      None
  ) lines in

  match ws_key with
  | None ->
    Log.traceln "Local: No Sec-WebSocket-Key in request";
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
    Log.traceln "Local: WebSocket handshake completed (path %s)" path;
    if String.length leftover > 0 then
      Log.traceln "Local: %d leftover bytes after handshake headers" (String.length leftover);
    Ok (path, leftover)

(* Handle a client connection *)
let handle_client ~clock ~on_subscribe flow _addr =
  (* Ensure flow is always closed when we're done, regardless of how we exit *)
  Fun.protect ~finally:(fun () ->
    (try Eio.Flow.close flow with _ -> ())
  ) @@ fun () ->
  (* Perform WebSocket handshake *)
  match websocket_handshake flow with
  | Error _ -> ()
  | Ok (path, leftover) ->
    let cluster = Cluster.of_path path in
    let client =
      add_client ~cluster (flow :> Eio.Flow.two_way_ty Eio.Resource.t)
    in
    let leftover_ref = ref leftover in

    (* Send Massive-style connected status *)
    ignore (send_to_client ~clock client (status_message "connected" "Connected to relay"));

    let rec loop () =
      match Websocket.Frame.decode_buffered flow leftover_ref with
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
            | "auth" ->
              (* Auth always succeeds for local relay *)
              Log.traceln "Local: Client %d authenticated" client.id;
              ignore (send_to_client ~clock client (status_message "auth_success" "Authenticated"))
            | "subscribe" ->
              let symbols = parse_params req.params in
              update_subscriptions client symbols;
              on_subscribe ~cluster symbols;
              Log.traceln "Local: Client %d [%s] subscribed to: %s"
                client.id (Cluster.to_string cluster)
                (String.concat ", " symbols);
              ignore (send_to_client ~clock client (status_message "success" "Subscribed"))
            | "unsubscribe" ->
              let symbols = parse_params req.params in
              Eio.Mutex.use_rw clients_mutex ~protect:true (fun () ->
                client.subscribed_symbols <- List.filter
                  (fun s -> not (List.mem s symbols))
                  client.subscribed_symbols
              );
              ignore (send_to_client ~clock client (status_message "success" "Unsubscribed"))
            | _ ->
              Log.traceln "Local: Unknown action: %s" req.action;
              ignore (send_to_client ~clock client (status_message "error" ("Unknown action: " ^ req.action)))
          with e ->
            Log.traceln "Local: Error parsing client message: %s" (Printexc.to_string e);
            ignore (send_to_client ~clock client (status_message "error" "Invalid message format")));
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
  let clock = Eio.Stdenv.clock env in
  let addr_v4 = `Tcp (Eio.Net.Ipaddr.V4.loopback, port) in
  let addr_v6 = `Tcp (Eio.Net.Ipaddr.V6.loopback, port) in

  Log.traceln "Local: Starting WebSocket server on ws://localhost:%d" port;

  let socket_v4 = Eio.Net.listen net ~sw ~backlog:10 ~reuse_addr:true addr_v4 in
  let socket_v6 = Eio.Net.listen net ~sw ~backlog:10 ~reuse_addr:true addr_v6 in

  let on_error e =
    Log.traceln "Local: Client error: %s" (Printexc.to_string e)
  in

  let rec accept_loop socket =
    Eio.Net.accept_fork socket ~sw ~on_error (fun flow addr ->
      handle_client ~clock ~on_subscribe flow addr
    );
    accept_loop socket
  in

  Eio.Fiber.fork ~sw (fun () -> accept_loop socket_v4);
  Eio.Fiber.fork ~sw (fun () -> accept_loop socket_v6)

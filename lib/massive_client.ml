(* Massive WebSocket client for real-time market data *)
(* Connects to Massive API (wss://socket.polygon.io/stocks) *)

(* Massive WebSocket authentication message *)
type auth_message = {
  action : string;
  params : string;
}
[@@deriving yojson]

(* Massive WebSocket subscription message *)
type subscribe_message = {
  action : string;
  params : string;  (* Comma-separated channels like "A.AAPL,Q.AAPL,A.MSFT,Q.MSFT" *)
}
[@@deriving yojson]

(* Massive status message *)
type status_message = {
  ev : string;          (* Event type: "status" *)
  status : string;      (* e.g., "auth_success", "connected" *)
  message : string;
}
[@@deriving show, yojson] [@@yojson.allow_extra_fields]

(* Relay only cares about status vs data-to-forward.
   Data messages carry the raw JSON to avoid re-serialization on broadcast. *)
type massive_message =
  | Status of status_message
  | Data of { symbol : string; raw_json : Yojson.Safe.t }
  | Unknown of string

(* WebSocket client for Massive *)
module Client = struct
  type t = {
    conn : Websocket.Connection.t;
    massive_key : string;
    cluster : Cluster.t;
    mutable subscribed_symbols : string list;
    mutable reconnect_attempts : int;
  }

  (* Exponential backoff configuration. Delays double each attempt up to
     [max_delay] (1 hour), for [max_reconnect_attempts] attempts per burst.
     When a burst is exhausted the caller's outer loop restarts it from
     attempt 0, so the relay keeps trying indefinitely but at steady state
     hits the upstream at most once per hour. *)
  let base_delay = 1.0
  let max_delay = 3600.0
  let max_reconnect_attempts = 15

  let calculate_backoff_delay attempt =
    (* 2^12 = 4096 > max_delay, so the exponent is capped at 12 to reach the
       1-hour ceiling without overflowing the float shift. *)
    let delay = base_delay *. (2.0 ** Float.of_int (min attempt 12)) in
    Float.min delay max_delay

  (* Connect to Massive WebSocket with retry logic. [cluster] selects the upstream
     endpoint path (/stocks, /futures). *)
  let rec connect_with_retry ~sw ~env ~massive_key ~cluster ~attempt () =
    let url = Uri.of_string (Cluster.upstream_url cluster) in
    let authenticator = Https.authenticator () in

    if attempt > 0 then
      Log.traceln "Massive[%s]: Connection attempt %d/%d" (Cluster.to_string cluster) attempt max_reconnect_attempts;

    Log.traceln "Massive[%s]: Connecting to %s..." (Cluster.to_string cluster) (Uri.to_string url);

    match Websocket.Connection.handshake ~sw ~env ~authenticator url with
    | Ok conn ->
      Log.traceln "Massive[%s]: Connected successfully" (Cluster.to_string cluster);
      Ok {
        conn;
        massive_key;
        cluster;
        subscribed_symbols = [];
        reconnect_attempts = attempt;
      }
    | Error e when attempt < max_reconnect_attempts ->
      let delay = calculate_backoff_delay attempt in
      Log.traceln "Massive: Connection failed: %s"
        (match e with
         | `InvalidScheme s -> "Invalid scheme: " ^ s
         | `InvalidUrl s -> "Invalid URL: " ^ s
         | `DnsError s -> "DNS error: " ^ s
         | `TlsError s -> "TLS error: " ^ s
         | `HandshakeError s -> "Handshake error: " ^ s);
      Log.traceln "Massive: Retrying in %.1f seconds" delay;
      Eio.Time.sleep (Eio.Stdenv.clock env) delay;
      connect_with_retry ~sw ~env ~massive_key ~cluster ~attempt:(attempt + 1) ()
    | Error e ->
      Log.traceln "Massive: Max reconnection attempts reached";
      Error e

  (* Initial connection with authentication, to a specific [cluster]. *)
  let connect ~sw ~env ~massive_key ~cluster () =
    let ( let* ) = Result.( let* ) in
    let* client =
      connect_with_retry ~sw ~env ~massive_key ~cluster ~attempt:0 ()
    in

    (* Send authentication message *)
    let auth_msg : auth_message = {
      action = "auth";
      params = massive_key;
    } in
    let json = yojson_of_auth_message auth_msg in
    let msg_str = Yojson.Safe.to_string json in

    Log.traceln "Massive: Sending authentication";
    let* () = Websocket.Connection.send_text client.conn msg_str in

    (* Wait for auth confirmation *)
    let rec wait_for_auth retry_count =
      if retry_count > 50 then
        Error (`HandshakeError "Timed out waiting for auth confirmation")
      else
        let* frame = Websocket.Connection.receive client.conn in
        match frame.Websocket.Frame.opcode with
        | Text ->
          (try
            let json = Yojson.Safe.from_string frame.payload in
            let messages = Yojson.Safe.Util.to_list json in
            let rec check_messages msgs =
              match msgs with
              | [] -> wait_for_auth (retry_count + 1)
              | msg :: rest ->
                (try
                  let status_msg : status_message = status_message_of_yojson msg in
                  if String.equal status_msg.ev "status" &&
                     String.equal status_msg.status "auth_success" then begin
                    Log.traceln "Massive: Authentication successful";
                    Ok ()
                  end else if String.equal status_msg.ev "status" &&
                              String.equal status_msg.status "auth_failed" then
                    Error (`HandshakeError ("Authentication failed: " ^ status_msg.message))
                  else
                    check_messages rest
                with _ ->
                  check_messages rest)
            in
            check_messages messages
          with e ->
            Log.traceln "Massive: Error parsing auth response: %s" (Printexc.to_string e);
            wait_for_auth (retry_count + 1))
        | Ping ->
          let pong_frame = Websocket.Frame.{
            fin = true;
            opcode = Pong;
            mask = true;
            payload = frame.payload;
          } in
          let encoded = Websocket.Frame.encode pong_frame in
          (* A failed pong write here means the connection died mid-auth —
             report it so connect() retries rather than crashing. *)
          (try
            Eio.Flow.copy_string encoded client.conn.flow;
            wait_for_auth retry_count
          with
          | Eio.Cancel.Cancelled _ as e -> raise e
          | _ -> Error `ConnectionClosed)
        | _ -> wait_for_auth (retry_count + 1)
    in

    let* () = wait_for_auth 0 in
    Ok client

  (* Subscribe to symbols *)
  let subscribe client symbols =
    let ( let* ) = Result.( let* ) in

    (* Forward channel strings as-is (e.g. "A.AAPL,Q.AAPL,A.MSFT,Q.MSFT") *)
    let params = String.concat "," symbols in

    let sub_msg = {
      action = "subscribe";
      params = params;
    } in

    let json = yojson_of_subscribe_message sub_msg in
    let msg_str = Yojson.Safe.to_string json in

    Log.traceln "Massive: Subscribing to %d symbols" (List.length symbols);

    let* () = Websocket.Connection.send_text client.conn msg_str in
    client.subscribed_symbols <-
      List.sort_uniq ~cmp:String.compare (symbols @ client.subscribed_symbols);
    Ok ()

  (* Parse a message from JSON.
     Extract the symbol for tracking — keep raw JSON to avoid
     re-serialization on broadcast. *)
  let parse_message json =
    let ev_type = match Yojson.Safe.Util.member "ev" json with
      | `String s -> s
      | _ -> "unknown"
    in
    match ev_type with
    | "status" ->
      (try Status (status_message_of_yojson json)
       with _ -> Unknown ev_type)
    | _ ->
      (* Any non-status message with a sym field is data to forward *)
      (match Yojson.Safe.Util.member "sym" json with
       | `String symbol -> Data { symbol; raw_json = json }
       | _ -> Unknown ev_type)

  (* Receive next frame *)
  let receive client =
    let ( let* ) = Result.( let* ) in
    let* frame = Websocket.Connection.receive client.conn in

    match frame.Websocket.Frame.opcode with
    | Text ->
      (try
        let json = Yojson.Safe.from_string frame.payload in
        let messages = Yojson.Safe.Util.to_list json in
        let parsed = List.map parse_message messages in
        Ok (`Messages parsed)
      with e ->
        Error (`ParseError (Printexc.to_string e, frame.payload)))
    | Ping ->
      let pong_frame = Websocket.Frame.{
        fin = true;
        opcode = Pong;
        mask = true;
        payload = frame.payload;
      } in
      let encoded = Websocket.Frame.encode pong_frame in
      (* A failed pong write means the socket is dead — surface it as a closed
         connection so the caller reconnects, rather than throwing. *)
      (try
        Eio.Flow.copy_string encoded client.conn.flow;
        Ok `Ping
      with
      | Eio.Cancel.Cancelled _ as e -> raise e
      | _ -> Error `ConnectionClosed)
    | Close ->
      Log.traceln "Massive: Received CLOSE frame";
      Error `ConnectionClosed
    | _ ->
      Ok `Other

  let close client =
    Log.traceln "Massive: Closing connection";
    Websocket.Connection.close client.conn

  (* Reconnect and resubscribe *)
  let reconnect ~sw ~env client =
    let ( let* ) = Result.( let* ) in

    (* Close old connection first to release file descriptor *)
    close client;

    Log.traceln "Massive[%s]: Attempting to reconnect..." (Cluster.to_string client.cluster);
    let* new_client =
      connect ~sw ~env ~massive_key:client.massive_key ~cluster:client.cluster ()
    in

    (* Update reconnect count *)
    new_client.reconnect_attempts <- client.reconnect_attempts + 1;

    (* Resubscribe to previous symbols if any *)
    if List.length client.subscribed_symbols > 0 then begin
      Log.traceln "Massive: Resubscribing to %d symbols after reconnect"
        (List.length client.subscribed_symbols);
      let* () = subscribe new_client client.subscribed_symbols in
      Ok new_client
    end else
      Ok new_client
end

(* Polygon WebSocket client for real-time market data *)
(* Adapted from longleaf's massive_websocket.ml *)

(* Polygon WebSocket authentication message *)
type auth_message = {
  action : string;
  params : string;
}
[@@deriving yojson]

(* Polygon WebSocket subscription message *)
type subscribe_message = {
  action : string;
  params : string;  (* Comma-separated channels like "A.AAPL,A.MSFT" *)
}
[@@deriving yojson]

(* Polygon status message *)
type status_message = {
  ev : string;          (* Event type: "status" *)
  status : string;      (* e.g., "auth_success", "connected" *)
  message : string;
}
[@@deriving show, yojson] [@@yojson.allow_extra_fields]

(* Polygon aggregate per second message *)
type aggregate_message = {
  ev : string;          (* Event type: "A" for aggregates per second *)
  sym : string;         (* Stock ticker symbol *)
  v : int;              (* Tick volume *)
  av : int;             (* Accumulated volume for the day *)
  op : float;           (* Official opening price *)
  vw : float;           (* Volume-weighted average price *)
  o : float;            (* Open price for this aggregate *)
  c : float;            (* Close price for this aggregate *)
  h : float;            (* High price for this aggregate *)
  l : float;            (* Low price for this aggregate *)
  a : float;            (* Day's volume-weighted average price *)
  z : int;              (* Average trade size *)
  s : int;              (* Start timestamp (Unix milliseconds) *)
  e : int;              (* End timestamp (Unix milliseconds) *)
  otc : bool option; [@yojson.option]  (* OTC ticker indicator *)
}
[@@deriving show, yojson]

(* Parse message as aggregate or status *)
type polygon_message =
  | Status of status_message
  | Aggregate of aggregate_message
  | Unknown of string

(* WebSocket client for Polygon *)
module Client = struct
  type t = {
    conn : Websocket.Connection.t;
    polygon_key : string;
    mutable subscribed_symbols : string list;
    mutable reconnect_attempts : int;
  }

  (* Exponential backoff configuration *)
  let base_delay = 1.0
  let max_delay = 60.0
  let max_reconnect_attempts = 10

  let calculate_backoff_delay attempt =
    let delay = base_delay *. (2.0 ** Float.of_int (min attempt 6)) in
    Float.min delay max_delay

  (* Connect to Polygon WebSocket with retry logic *)
  let rec connect_with_retry ~sw ~env ~polygon_key ~attempt () =
    let url = Uri.of_string "wss://socket.polygon.io/stocks" in
    let authenticator = Https.authenticator () in

    if attempt > 0 then
      Eio.traceln "Polygon: Connection attempt %d/%d" attempt max_reconnect_attempts;

    Eio.traceln "Polygon: Connecting to %s..." (Uri.to_string url);

    match Websocket.Connection.handshake ~sw ~env ~authenticator url with
    | Ok conn ->
      Eio.traceln "Polygon: Connected successfully";
      Ok {
        conn;
        polygon_key;
        subscribed_symbols = [];
        reconnect_attempts = attempt;
      }
    | Error e when attempt < max_reconnect_attempts ->
      let delay = calculate_backoff_delay attempt in
      Eio.traceln "Polygon: Connection failed: %s"
        (match e with
         | `InvalidScheme s -> "Invalid scheme: " ^ s
         | `InvalidUrl s -> "Invalid URL: " ^ s
         | `DnsError s -> "DNS error: " ^ s
         | `TlsError s -> "TLS error: " ^ s
         | `HandshakeError s -> "Handshake error: " ^ s);
      Eio.traceln "Polygon: Retrying in %.1f seconds" delay;
      Eio.Time.sleep (Eio.Stdenv.clock env) delay;
      connect_with_retry ~sw ~env ~polygon_key ~attempt:(attempt + 1) ()
    | Error e ->
      Eio.traceln "Polygon: Max reconnection attempts reached";
      Error e

  (* Initial connection with authentication *)
  let connect ~sw ~env ~polygon_key () =
    let ( let* ) = Result.( let* ) in
    let* client = connect_with_retry ~sw ~env ~polygon_key ~attempt:0 () in

    (* Send authentication message *)
    let auth_msg : auth_message = {
      action = "auth";
      params = polygon_key;
    } in
    let json = yojson_of_auth_message auth_msg in
    let msg_str = Yojson.Safe.to_string json in

    Eio.traceln "Polygon: Sending authentication";
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
                    Eio.traceln "Polygon: Authentication successful";
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
            Eio.traceln "Polygon: Error parsing auth response: %s" (Printexc.to_string e);
            wait_for_auth (retry_count + 1))
        | Ping ->
          let pong_frame = Websocket.Frame.{
            fin = true;
            opcode = Pong;
            mask = true;
            payload = frame.payload;
          } in
          let encoded = Websocket.Frame.encode pong_frame in
          Eio.Flow.copy_string encoded client.conn.flow;
          wait_for_auth retry_count
        | _ -> wait_for_auth (retry_count + 1)
    in

    let* () = wait_for_auth 0 in
    Ok client

  (* Subscribe to symbols *)
  let subscribe client symbols =
    let ( let* ) = Result.( let* ) in

    (* Build subscription params: "A.AAPL,A.MSFT,A.TSLA" *)
    let params =
      List.map (fun sym -> "A." ^ sym) symbols
      |> String.concat ","
    in

    let sub_msg = {
      action = "subscribe";
      params = params;
    } in

    let json = yojson_of_subscribe_message sub_msg in
    let msg_str = Yojson.Safe.to_string json in

    Eio.traceln "Polygon: Subscribing to %d symbols" (List.length symbols);

    let* () = Websocket.Connection.send_text client.conn msg_str in
    client.subscribed_symbols <- symbols @ client.subscribed_symbols;
    Ok ()

  (* Parse a message from JSON *)
  let parse_message json =
    let ev_type = match Yojson.Safe.Util.member "ev" json with
      | `String s -> s
      | _ -> "unknown"
    in
    match ev_type with
    | "status" ->
      (try Status (status_message_of_yojson json)
       with _ -> Unknown ev_type)
    | "A" | "AS" ->
      (try Aggregate (aggregate_message_of_yojson json)
       with _ -> Unknown ev_type)
    | _ -> Unknown ev_type

  (* Receive next frame *)
  let receive client =
    let ( let* ) = Result.( let* ) in
    let* frame = Websocket.Connection.receive client.conn in

    match frame.Websocket.Frame.opcode with
    | Text ->
      Eio.traceln "Polygon: Frame received len=%d" (String.length frame.payload);
      (try
        let json = Yojson.Safe.from_string frame.payload in
        let messages = Yojson.Safe.Util.to_list json in
        let parsed = List.map parse_message messages in
        Ok (`Messages parsed)
      with e ->
        Error (`ParseError (Printexc.to_string e)))
    | Ping ->
      let pong_frame = Websocket.Frame.{
        fin = true;
        opcode = Pong;
        mask = true;
        payload = frame.payload;
      } in
      let encoded = Websocket.Frame.encode pong_frame in
      Eio.Flow.copy_string encoded client.conn.flow;
      Ok `Ping
    | Close ->
      Eio.traceln "Polygon: Received CLOSE frame";
      Error `ConnectionClosed
    | _ ->
      Ok `Other

  let close client =
    Eio.traceln "Polygon: Closing connection";
    Websocket.Connection.close client.conn
end

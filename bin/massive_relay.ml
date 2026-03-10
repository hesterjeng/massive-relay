(* Massive relay service *)

module Args = struct
  let default_port = 9999
  let default_massive_key = Sys.getenv_opt "MASSIVE_KEY"

  let port_arg =
    let doc = "Port for local clients to connect (default 9999)" in
    Cmdliner.Arg.(value & opt int default_port & info ["port"; "p"] ~doc)

  let massive_key_arg =
    let doc = "Massive API key (default from MASSIVE_KEY env var)" in
    Cmdliner.Arg.(value & opt (some string) default_massive_key & info ["key"; "k"] ~doc)
end

module Relay = struct
  (* Pending subscriptions that need to be sent to Massive *)
  let pending_symbols : string list ref = ref []
  let pending_mutex = Eio.Mutex.create ()

  (* Add symbols to pending subscriptions *)
  let add_pending_symbols symbols =
    Eio.Mutex.use_rw pending_mutex ~protect:true (fun () ->
      pending_symbols := List.sort_uniq ~cmp:Stdlib.compare (!pending_symbols @ symbols)
    )

  (* Get and clear pending symbols *)
  let take_pending_symbols () =
    Eio.Mutex.use_rw pending_mutex ~protect:true (fun () ->
      let symbols = !pending_symbols in
      pending_symbols := [];
      symbols
    )

  let format_error = function
    | `HandshakeError s -> s
    | `InvalidScheme s -> "Invalid scheme: " ^ s
    | `InvalidUrl s -> "Invalid URL: " ^ s
    | `DnsError s -> "DNS error: " ^ s
    | `TlsError s -> "TLS error: " ^ s
    | `WriteError s -> "Write error: " ^ s
    | `ReadError s -> "Read error: " ^ s
    | `InvalidOpcode i -> "Invalid opcode: " ^ string_of_int i
    | `ConnectionClosed -> "Connection closed"

  (* Main relay loop *)
  let run ~sw ~env ~massive_key ~local_port =
    (* Start local server *)
    Massive_relay.Local_server.start ~sw ~env ~port:local_port
      ~on_subscribe:(fun symbols -> add_pending_symbols symbols);

    (* Connect to Massive with reconnection loop *)
    let rec connect_loop () =
      Eio.traceln "Relay: Connecting to Massive...";
      match Massive_relay.Massive_client.Client.connect ~sw ~env ~massive_key () with
      | Error e ->
        Eio.traceln "Relay: Failed to connect to Massive: %s" (format_error e);
        Eio.traceln "Relay: Retrying in 5 seconds...";
        Eio.Time.sleep (Eio.Stdenv.clock env) 5.0;
        connect_loop ()
      | Ok client ->
        Eio.traceln "Relay: Connected to Massive, starting main loop";
        run_with_client ~sw ~env client

    and run_with_client ~sw ~env client =
      let client_ref = ref client in
      let clock = Eio.Stdenv.clock env in
      let last_data_time = ref (Eio.Time.now clock) in

      (* Upstream data staleness: if we receive only pings and no actual data
         for this long, the upstream connection is alive but not delivering.
         Reconnect to get a fresh upstream session. *)
      let data_staleness_sec = 120.0 in

      (* Background fiber to handle new subscriptions *)
      Eio.Fiber.fork ~sw (fun () ->
        while true do
          Eio.Time.sleep clock 1.0;
          let new_symbols = take_pending_symbols () in
          if List.length new_symbols > 0 then begin
            Eio.traceln "Relay: Subscribing to %d new symbols" (List.length new_symbols);
            match Massive_relay.Massive_client.Client.subscribe !client_ref new_symbols with
            | Ok () -> ()
            | Error _ ->
              Eio.traceln "Relay: Failed to subscribe, will retry";
              add_pending_symbols new_symbols
          end
        done
      );

      (* Timeout for upstream receive. If nothing at all arrives (not even pings)
         for this long, the TCP connection is dead. *)
      let receive_timeout_sec = 120.0 in

      (* Main receive loop with reconnection *)
      let rec loop () =
        let receive_result =
          match
            Eio.Time.with_timeout_exn clock receive_timeout_sec (fun () ->
              Massive_relay.Massive_client.Client.receive !client_ref)
          with
          | result -> result
          | exception Eio.Time.Timeout ->
            Eio.traceln "Relay: Upstream receive timeout (%.0fs), reconnecting..." receive_timeout_sec;
            Error (`ReadError "receive timeout")
        in
        match receive_result with
        | Ok (`Messages msgs) ->
          last_data_time := Eio.Time.now clock;
          msgs |> List.iter (fun msg ->
            match msg with
            | Massive_relay.Massive_client.Status status ->
              Eio.traceln "Relay: Status - %s: %s" status.status status.message
            | Massive_relay.Massive_client.Aggregate { raw_json; _ } ->
              Massive_relay.Local_server.broadcast_aggregate
                (Yojson.Safe.to_string raw_json)
            | Massive_relay.Massive_client.Unknown _ -> ()
          );
          loop ()
        | Ok `Ping ->
          let age = Eio.Time.now clock -. !last_data_time in
          if Float.(age > data_staleness_sec) then begin
            Eio.traceln "Relay: Upstream stale (%.0fs of pings, no data), reconnecting..." age;
            reconnect_and_loop ()
          end else
            loop ()
        | Ok `Other -> loop ()
        | Error `ConnectionClosed ->
          Eio.traceln "Relay: Connection closed, reconnecting...";
          reconnect_and_loop ()
        | Error (`ParseError e) ->
          Eio.traceln "Relay: Parse error: %s" e;
          loop ()
        | Error (`ReadError e) ->
          Eio.traceln "Relay: Read error: %s (reconnecting...)" e;
          reconnect_and_loop ()
        | Error (`InvalidOpcode i) ->
          Eio.traceln "Relay: Invalid opcode: %d (reconnecting...)" i;
          reconnect_and_loop ()

      and reconnect_and_loop () =
        match Massive_relay.Massive_client.Client.reconnect ~sw ~env !client_ref with
        | Ok new_client ->
          client_ref := new_client;
          last_data_time := Eio.Time.now clock;
          Eio.traceln "Relay: Reconnected successfully";
          loop ()
        | Error e ->
          Eio.traceln "Relay: Reconnection failed: %s" (format_error e);
          Eio.traceln "Relay: Retrying in 5 seconds...";
          Eio.Time.sleep clock 5.0;
          reconnect_and_loop ()
      in
      loop ()
    in
    connect_loop ()
end

module Cmd = struct
  let run port massive_key =
    (* Initialize RNG before any crypto operations *)
    Massive_relay.Https.init_rng ();
    match massive_key with
    | None ->
      Eio.traceln "Error: No Massive API key provided.";
      Eio.traceln "Set MASSIVE_KEY environment variable or use --key option."
    | Some key ->
      Eio.traceln "Massive Relay starting...";
      Eio.traceln "  Local port: %d" port;
      Eio_main.run @@ fun env ->
      Eio.Switch.run @@ fun sw ->
      Relay.run ~sw ~env ~massive_key:key ~local_port:port

  let top =
    let term = Cmdliner.Term.(const run $ Args.port_arg $ Args.massive_key_arg) in
    let doc = "Massive WebSocket relay - share one connection among multiple clients" in
    let info = Cmdliner.Cmd.info ~doc "massive-relay" in
    Cmdliner.Cmd.v info term
end

let () = Stdlib.exit @@ Cmdliner.Cmd.eval Cmd.top

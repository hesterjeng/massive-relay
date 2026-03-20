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

  let log_file_arg =
    let doc = "Log file path (default: massive-relay.log)" in
    Cmdliner.Arg.(value & opt string "massive-relay.log" & info ["log-file"; "l"] ~doc)
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
      Massive_relay.Log.traceln "Relay: Connecting to Massive...";
      match Massive_relay.Massive_client.Client.connect ~sw ~env ~massive_key () with
      | Error e ->
        Massive_relay.Log.traceln "Relay: Failed to connect to Massive: %s" (format_error e);
        Massive_relay.Log.traceln "Relay: Retrying in 5 seconds...";
        Eio.Time.sleep (Eio.Stdenv.clock env) 5.0;
        connect_loop ()
      | Ok client ->
        Massive_relay.Log.traceln "Relay: Connected to Massive, starting main loop";
        run_with_client ~sw ~env client

    and run_with_client ~sw ~env client =
      let client_ref = ref client in
      let clock = Eio.Stdenv.clock env in
      (* None until first real data arrives. Staleness check only applies
         after we've seen data — avoids reconnect churn outside market hours. *)
      let last_data_time = ref None in
      let data_staleness_sec = 120.0 in
      let msg_count = ref 0 in
      let ping_count = ref 0 in
      (* Per-symbol last-seen tracking for dropout detection *)
      let symbol_last_seen : (string, float) Hashtbl.t = Hashtbl.create 128 in
      let last_symbol_check = ref 0.0 in

      (* Background fiber to handle new subscriptions *)
      Eio.Fiber.fork ~sw (fun () ->
        while true do
          Eio.Time.sleep clock 1.0;
          let new_symbols = take_pending_symbols () in
          if List.length new_symbols > 0 then begin
            Massive_relay.Log.traceln "Relay: Subscribing to %d new symbols" (List.length new_symbols);
            match Massive_relay.Massive_client.Client.subscribe !client_ref new_symbols with
            | Ok () -> ()
            | Error _ ->
              Massive_relay.Log.traceln "Relay: Failed to subscribe, will retry";
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
            Massive_relay.Log.traceln "Relay: Upstream receive timeout (%.0fs), reconnecting..." receive_timeout_sec;
            Error (`ReadError "receive timeout")
        in
        match receive_result with
        | Ok (`Messages msgs) ->
          let now = Eio.Time.now clock in
          let has_aggregates = List.exists (fun msg ->
            match msg with
            | Massive_relay.Massive_client.Aggregate _ -> true
            | _ -> false) msgs in
          if has_aggregates then
            last_data_time := Some now;
          let n = List.length msgs in
          msg_count := !msg_count + n;
          if !msg_count >= 5000 then begin
            Massive_relay.Log.traceln "Relay: upstream flowing (%d msgs, %d pings since last, %d symbols tracked)"
              !msg_count !ping_count (Hashtbl.length symbol_last_seen);
            msg_count := 0;
            ping_count := 0
          end;
          msgs |> List.iter (fun msg ->
            match msg with
            | Massive_relay.Massive_client.Status status ->
              Massive_relay.Log.traceln "Relay: Status - %s: %s" status.status status.message
            | Massive_relay.Massive_client.Aggregate { symbol; raw_json } ->
              Hashtbl.replace symbol_last_seen symbol now;
              Massive_relay.Local_server.broadcast_aggregate
                (Yojson.Safe.to_string raw_json)
            | Massive_relay.Massive_client.Unknown _ -> ()
          );
          (* Periodic symbol dropout check - every 60s, only when data is flowing *)
          let data_flowing = match !last_data_time with
            | Some t -> Float.(now -. t < 30.0)
            | None -> false in
          if data_flowing && Float.(now -. !last_symbol_check > 60.0)
             && Hashtbl.length symbol_last_seen > 0 then begin
            last_symbol_check := now;
            let total = Hashtbl.length symbol_last_seen in
            let silent = Hashtbl.fold (fun sym last acc ->
              let age = now -. last in
              if Float.(age > 30.0) then (sym, age) :: acc else acc
            ) symbol_last_seen [] in
            if List.length silent > 0 then begin
              let sorted = List.sort (fun (_, a) (_, b) -> Float.compare b a) silent in
              let display = List.take 10 sorted
                |> List.map (fun (sym, age) -> Printf.sprintf "%s(%.0fs)" sym age)
                |> String.concat ", " in
              Massive_relay.Log.traceln "Relay: SYMBOL_DROPOUT %d/%d symbols silent >30s: %s"
                (List.length silent) total display
            end
          end;
          loop ()
        | Ok `Ping ->
          incr ping_count;
          (match !last_data_time with
          | None -> loop ()  (* Never received data — market likely closed *)
          | Some t ->
            let age = Eio.Time.now clock -. t in
            if Float.(age > data_staleness_sec) then begin
              Massive_relay.Log.traceln "Relay: Upstream stale (%.0fs of pings, no data), reconnecting..." age;
              reconnect_and_loop ()
            end else
              loop ())
        | Ok `Other -> loop ()
        | Error `ConnectionClosed ->
          Massive_relay.Log.traceln "Relay: Connection closed, reconnecting...";
          reconnect_and_loop ()
        | Error (`ParseError e) ->
          Massive_relay.Log.traceln "Relay: Parse error: %s" e;
          loop ()
        | Error (`ReadError e) ->
          Massive_relay.Log.traceln "Relay: Read error: %s (reconnecting...)" e;
          reconnect_and_loop ()
        | Error (`InvalidOpcode i) ->
          Massive_relay.Log.traceln "Relay: Invalid opcode: %d (reconnecting...)" i;
          reconnect_and_loop ()

      and reconnect_and_loop () =
        match Massive_relay.Massive_client.Client.reconnect ~sw ~env !client_ref with
        | Ok new_client ->
          client_ref := new_client;
          last_data_time := None;
          Massive_relay.Log.traceln "Relay: Reconnected successfully";
          loop ()
        | Error e ->
          Massive_relay.Log.traceln "Relay: Reconnection failed: %s" (format_error e);
          Massive_relay.Log.traceln "Relay: Retrying in 5 seconds...";
          Eio.Time.sleep clock 5.0;
          reconnect_and_loop ()
      in
      loop ()
    in
    connect_loop ()
end

module Cmd = struct
  let run port massive_key log_file =
    (* Initialize logging first *)
    Massive_relay.Log.init log_file;
    (* Initialize RNG before any crypto operations *)
    Massive_relay.Https.init_rng ();
    match massive_key with
    | None ->
      Massive_relay.Log.traceln "Error: No Massive API key provided.";
      Massive_relay.Log.traceln "Set MASSIVE_KEY environment variable or use --key option."
    | Some key ->
      Massive_relay.Log.traceln "Massive Relay starting...";
      Massive_relay.Log.traceln "  Log file: %s" log_file;
      Massive_relay.Log.traceln "  Local port: %d" port;
      Eio_main.run @@ fun env ->
      Eio.Switch.run @@ fun sw ->
      Relay.run ~sw ~env ~massive_key:key ~local_port:port

  let top =
    let term = Cmdliner.Term.(const run $ Args.port_arg $ Args.massive_key_arg $ Args.log_file_arg) in
    let doc = "Massive WebSocket relay - share one connection among multiple clients" in
    let info = Cmdliner.Cmd.info ~doc "massive-relay" in
    Cmdliner.Cmd.v info term
end

let () = Stdlib.exit @@ Cmdliner.Cmd.eval Cmd.top

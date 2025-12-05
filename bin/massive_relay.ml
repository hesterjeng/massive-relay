(* Massive relay service *)

module Args = struct
  let default_port = 9999
  let default_polygon_key = Sys.getenv_opt "POLYGON_KEY"

  let port_arg =
    let doc = "Port for local clients to connect (default 9999)" in
    Cmdliner.Arg.(value & opt int default_port & info ["port"; "p"] ~doc)

  let polygon_key_arg =
    let doc = "Polygon API key (default from POLYGON_KEY env var)" in
    Cmdliner.Arg.(value & opt (some string) default_polygon_key & info ["key"; "k"] ~doc)
end

module Relay = struct
  (* Pending subscriptions that need to be sent to Polygon *)
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

  (* Main relay loop *)
  let run ~sw ~env ~polygon_key ~local_port =
    (* Start local server *)
    Massive_relay.Local_server.start ~sw ~env ~port:local_port
      ~on_subscribe:(fun symbols -> add_pending_symbols symbols);

    (* Connect to Polygon *)
    Eio.traceln "Relay: Connecting to Polygon...";
    match Massive_relay.Polygon_client.Client.connect ~sw ~env ~polygon_key () with
    | Error e ->
      Eio.traceln "Relay: Failed to connect to Polygon: %s"
        (match e with
         | `HandshakeError s -> s
         | `InvalidScheme s -> "Invalid scheme: " ^ s
         | `InvalidUrl s -> "Invalid URL: " ^ s
         | `DnsError s -> "DNS error: " ^ s
         | `TlsError s -> "TLS error: " ^ s
         | `WriteError s -> "Write error: " ^ s
         | `ReadError s -> "Read error: " ^ s
         | `InvalidOpcode i -> "Invalid opcode: " ^ string_of_int i
         | `ConnectionClosed -> "Connection closed")
    | Ok client ->
      Eio.traceln "Relay: Connected to Polygon, starting main loop";

      (* Background fiber to handle new subscriptions *)
      Eio.Fiber.fork ~sw (fun () ->
        while true do
          Eio.Time.sleep (Eio.Stdenv.clock env) 1.0;
          let new_symbols = take_pending_symbols () in
          if List.length new_symbols > 0 then begin
            Eio.traceln "Relay: Subscribing to %d new symbols" (List.length new_symbols);
            match Massive_relay.Polygon_client.Client.subscribe client new_symbols with
            | Ok () -> ()
            | Error _ -> Eio.traceln "Relay: Failed to subscribe"
          end
        done
      );

      (* Main receive loop *)
      let rec loop () =
        match Massive_relay.Polygon_client.Client.receive client with
        | Ok (`Messages msgs) ->
          msgs |> List.iter (fun msg ->
            match msg with
            | Massive_relay.Polygon_client.Status status ->
              Eio.traceln "Relay: Status - %s: %s" status.status status.message
            | Massive_relay.Polygon_client.Aggregate agg ->
              (* Broadcast to subscribed clients *)
              let json = Massive_relay.Polygon_client.yojson_of_aggregate_message agg in
              Massive_relay.Local_server.broadcast_aggregate (Yojson.Safe.to_string json)
            | Massive_relay.Polygon_client.Unknown _ -> ()
          );
          loop ()
        | Ok `Ping -> loop ()
        | Ok `Other -> loop ()
        | Error `ConnectionClosed ->
          Eio.traceln "Relay: Connection closed, exiting"
        | Error (`ParseError e) ->
          Eio.traceln "Relay: Parse error: %s" e;
          loop ()
        | Error (`ReadError e) ->
          Eio.traceln "Relay: Read error: %s" e
        | Error (`InvalidOpcode i) ->
          Eio.traceln "Relay: Invalid opcode: %d" i
      in
      loop ()
end

module Cmd = struct
  let run port polygon_key =
    match polygon_key with
    | None ->
      Eio.traceln "Error: No Polygon API key provided.";
      Eio.traceln "Set POLYGON_KEY environment variable or use --key option."
    | Some key ->
      Eio.traceln "Massive Relay starting...";
      Eio.traceln "  Local port: %d" port;
      Eio_main.run @@ fun env ->
      Eio.Switch.run @@ fun sw ->
      Relay.run ~sw ~env ~polygon_key:key ~local_port:port

  let top =
    let term = Cmdliner.Term.(const run $ Args.port_arg $ Args.polygon_key_arg) in
    let doc = "Polygon WebSocket relay - share one connection among multiple clients" in
    let info = Cmdliner.Cmd.info ~doc "massive-relay" in
    Cmdliner.Cmd.v info term
end

let () = Stdlib.exit @@ Cmdliner.Cmd.eval Cmd.top

(lang dune 3.0)
(name massive-relay)
(version 0.1.0)

(package
 (name massive-relay)
 (synopsis "Polygon/Massive WebSocket relay for sharing a single connection")
 (description "A relay service that connects to Polygon's WebSocket API and broadcasts market data to multiple local clients, allowing multiple trading strategies to share a single WebSocket connection.")
 (depends
  (ocaml (>= 5.0))
  dune
  eio
  eio_main
  yojson
  ppx_yojson_conv
  ppx_deriving
  cmdliner
  ptime))

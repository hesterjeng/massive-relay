(* A Massive market-data CLUSTER = a distinct WebSocket endpoint PATH (Massive
   serves /stocks, /futures, ... as separate sockets). The relay is a TRANSPARENT
   per-path multiplexer: a downstream client's connection path selects its cluster,
   the relay keeps ONE upstream connection per cluster, and each cluster's upstream
   data fans out only to that cluster's downstream clients. This mirrors how a client
   would hit Massive directly (host swap only) — the relay needs no symbol-format
   knowledge. *)

type t =
  | Stocks
  | Futures

(* Downstream request path -> cluster. Anything that isn't an explicit /futures is
   Stocks, so a legacy client connecting to ws://host:port with no path still works. *)
let of_path path =
  match String.lowercase_ascii (String.trim path) with
  | "/futures"
  | "futures" ->
    Futures
  | _ -> Stocks

(* Upstream Massive endpoint URL for this cluster. *)
let upstream_url = function
  | Stocks -> "wss://socket.polygon.io/stocks"
  | Futures -> "wss://socket.polygon.io/futures"

let to_string = function
  | Stocks -> "stocks"
  | Futures -> "futures"

let equal a b =
  match (a, b) with
  | Stocks, Stocks
  | Futures, Futures ->
    true
  | _ -> false

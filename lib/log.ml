(* File + stderr logger. Call [init] once at startup to enable file logging.
   All messages go to stderr; if a file is open they also go there. *)

let log_channel : out_channel option ref = ref None

let init filename =
  let oc = open_out_gen [Open_append; Open_creat; Open_wronly] 0o644 filename in
  log_channel := Some oc

let close () =
  match !log_channel with
  | Some oc -> close_out_noerr oc; log_channel := None
  | None -> ()

let traceln fmt =
  Format.kasprintf (fun msg ->
    let now = Unix.gettimeofday () in
    let tm = Unix.localtime now in
    let ms = int_of_float (mod_float now 1.0 *. 1000.0) in
    let timestamp = Printf.sprintf "%04d-%02d-%02d %02d:%02d:%02d.%03d"
      (tm.Unix.tm_year + 1900) (tm.Unix.tm_mon + 1) tm.Unix.tm_mday
      tm.Unix.tm_hour tm.Unix.tm_min tm.Unix.tm_sec ms in
    Printf.eprintf "[%s] %s\n%!" timestamp msg;
    (match !log_channel with
     | Some oc -> Printf.fprintf oc "[%s] %s\n%!" timestamp msg
     | None -> ())
  ) fmt
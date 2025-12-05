;; Guix manifest for massive-relay development environment
;; Only uses dependencies already in longleaf

(use-modules (guix packages)
             (gnu packages)
             (gnu packages compression)
             (gnu packages tls)
             (gnu packages pkg-config)
             (gnu packages longleaf-ocaml))

(specifications->manifest
 (list
       "gcc-toolchain"
       "ocaml"
       "dune"
       "ocamlformat"
       "ocaml-ptime"
       "ocaml-ppx-yojson-conv-lib"
       "ocaml-ppx-deriving"
       "ocaml-cmdliner"
       "ocaml-eio-main"
       "ocaml-yojson"
       "ocaml-cohttp-eio"
       "ocaml-ppx-yojson-conv"
       "ocaml-tls"
       "ocaml-x509"
       "ocaml-ca-certs"
       "ocaml-mirage-crypto-rng"
       "ocaml-tls-eio"
       "ocaml-base64"))

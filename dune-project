(lang dune 2.7)
(name kv_mmap_log)

;; for opam
(authors              "Tom Ridge <tom.j.ridge@googlemail.com>")
(maintainers          "Tom Ridge <tom.j.ridge@googlemail.com>")
(source               (github tomjridge/FIXME))
(homepage             https://github.com/tomjridge/FIXME)
(documentation        https://tomjridge.github.io/FIXME/ocamldoc/index.html)
(generate_opam_files  true)


(package 
 (name kv_mmap_log)
 (synopsis "A simple mmap'ed KV log")
 (depends ocamlfind dune ppx_jane 
          lwt lwt.unix))



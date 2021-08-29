
module Bigarray = Stdlib.Bigarray 

[@@@ warning "-32-33"]

open Bigarray

type buffer = (char, int8_unsigned_elt, c_layout) Array1.t 

type mmapped_file = { fd:Unix.file_descr; mutable buf:buffer }

let open_mmap ~sz ~fn = 
  let shared = true in
  Unix.(openfile fn [O_CREAT;O_RDWR]) 0o600 |> fun fd -> 
  Unix.map_file fd Char c_layout shared [| sz |] 
  |> array1_of_genarray |> fun buf -> 
  {fd;buf}

let close_mmap map = 
  map.buf <- Bigstringaf.create 0;
  Gc.full_major (); (* force flush of map *)
  Unix.close map.fd; 
  ()

let default_sz = 1_000_000_000 (* 1 GB *)

(* We can store the header in a separate file, or at the end of the
   mmapp'ed file *)

module Header = struct
  open Bin_prot.Std

  type header = { 
    tl         :int; 
    merged     :int; 
    hd         :int; 
    generation :int 
  }[@@deriving bin_io]

  let init_header () = { tl=0; merged=0; hd=0; generation=1 }

  (* ASSUMES header fits in this many bytes *)
  let header_sz = 1000

  let _ = assert(default_sz - header_sz > 0)

  (* NOTE tl,merged,hd measured in units of entry_sz, to make maths a bit easier *)

  let read_header map : header = 
    bin_read_header map.buf ~pos_ref:(ref (default_sz - header_sz))

  let write_header map h : unit = 
    let _i : int = bin_write_header map.buf ~pos:(default_sz - header_sz) h in
    ()
end
(* open Header *)


(** Writer log *)
type log_w = { mutable tl:int; mutable merged:int; mutable hd:int; mutable generation:int; map: mmapped_file }

let write_header log = 
  let {tl;merged;hd;generation;map} = log in
  Header.write_header map Header.{tl;merged;hd;generation}


module Make(S:sig
    type k[@@deriving bin_io]
    type v[@@deriving bin_io]
    val max_k_sz : int
    val max_v_sz : int
  end) = struct
  open S

  let entry_sz = 1+max_k_sz+max_v_sz

  type entry = Add of k*v | Del of k[@@deriving bin_io]
 
  let create ~fn = 
    open_mmap ~sz:default_sz ~fn |> fun map -> 
    let h = Header.init_header () in
    Header.write_header map h;
    {tl=h.tl;merged=h.merged;hd=h.hd;generation=h.generation;map}

  let open_ ~fn = 
    open_mmap ~sz:default_sz ~fn |> fun map -> 
    let h = Header.read_header map in
    {tl=h.tl;merged=h.merged;hd=h.hd;generation=h.generation;map}

  let close ~log = 
    write_header log;
    close_mmap log.map

  (* can write upto this offset, but not including *)
  let offset_limit = (default_sz - Header.header_sz)/entry_sz

  let mod_ n = n mod offset_limit

  let next_hd ~hd ~tl = 
    (* hd is assumed to be pointing where we just wrote an entry; we
       may need to wrap round, or there may be no space left available
       if we have reached tl *)
    let hd' = mod_ (hd+1) in
    match hd' = tl with 
    | true -> `Reached_tl
    | false -> `Ok hd'

  (* writer functions *)

  let add_entry log ent =
    let go hd' = 
      (* write at hd, and update *)
      let _n = bin_write_entry log.map.buf ~pos:(log.hd * entry_sz) ent in
      log.hd <- hd';
      write_header log;
      ()
    in
    match next_hd ~hd:log.hd ~tl:log.tl with
    | `Reached_tl -> 
      (* drop some entries and update tl and hd and generation *)
      let tl = mod_ (log.tl + offset_limit / 2) in
      log.tl <- tl;
      log.generation <- log.generation + 1;
      write_header log;
      (* following is just the `Ok case *)
      let hd' = 
        match next_hd ~hd:log.hd ~tl:log.tl with
        | `Ok hd' -> hd'
        | `Reached_tl -> assert(false)
      in
      go hd';
      ()              
    | `Ok hd' -> 
      go hd';
      ()

  let add log k v = add_entry log (Add(k,v))

  let del log k = add_entry log (Del(k))


  (* reader functions *)

  
  
end


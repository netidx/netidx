## Notes on bscript

- bscript is a dataflow language, when you write code you're actually
  specifying an event graph. This causes semantics to be very
  different than most programming languages in some cases.
  
- The examples directory contains bscript examples

- bscript struct fields are stored as arrays of pairs sorted by the
  field name, keep that in mind when using them in rust bindings.
  
- bscript modules and blocks are ; separated, the last item may not
  end in a semi. Comma separated items work in a similar way.

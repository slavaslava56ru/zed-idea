(identifier) @variable

(type_identifier) @type.go_struct

(type_spec
  name: (type_identifier) @variable)

(field_identifier) @property

(package_identifier) @variable

(package_clause
  (package_identifier) @package)

(qualified_type
  package: (package_identifier) @attribute
  name: (type_identifier) @type.go_struct)

((type_identifier) @type.builtin
  (#match? @type.builtin "^(any|bool|byte|comparable|complex64|complex128|error|float32|float64|int|int8|int16|int32|int64|rune|string|uint|uint8|uint16|uint32|uint64|uintptr)$"))

(parameter_declaration
  name: (identifier) @variable)

(variadic_parameter_declaration
  name: (identifier) @variable)

(method_declaration
  receiver: (parameter_list
    (parameter_declaration
      name: (identifier) @variable.receiver)))

(label_name) @label

(keyed_element
  .
  (literal_element
    (identifier) @property))

(selector_expression
  operand: (identifier) @variable)

(call_expression
  function: (identifier) @function.call)

(call_expression
  function: (selector_expression
    field: (field_identifier) @function.method.call))

(call_expression
  function: (selector_expression
    operand: (identifier) @attribute
    field: (field_identifier) @function.go_call))

((call_expression
  function: (identifier) @function.builtin)
  (#eq? @function.builtin "make"))

((call_expression
  function: (identifier) @keyword)
  (#eq? @keyword "append"))

(function_declaration
  name: (identifier) @function)

(method_declaration
  name: (field_identifier) @function.method)

(method_elem
  name: (field_identifier) @function.method)

[
  ";"
  "."
  ","
  ":"
] @punctuation.delimiter

[
  "("
  ")"
  "{"
  "}"
  "["
  "]"
] @punctuation.bracket

[
  "--"
  "-"
  "-="
  ":="
  "!"
  "!="
  "..."
  "*"
  "*"
  "*="
  "/"
  "/="
  "&"
  "&&"
  "&="
  "%"
  "%="
  "^"
  "^="
  "+"
  "++"
  "+="
  "<-"
  "<"
  "<<"
  "<<="
  "<="
  "="
  "=="
  ">"
  ">="
  ">>"
  ">>="
  "|"
  "|="
  "||"
  "~"
] @operator

[
  "break"
  "case"
  "chan"
  "const"
  "continue"
  "default"
  "defer"
  "else"
  "fallthrough"
  "for"
  "func"
  "go"
  "goto"
  "if"
  "import"
  "interface"
  "map"
  "package"
  "range"
  "return"
  "select"
  "struct"
  "switch"
  "type"
  "var"
] @keyword

[
  (interpreted_string_literal)
  (raw_string_literal)
  (rune_literal)
] @string

(escape_sequence) @string.escape

[
  (int_literal)
  (float_literal)
  (imaginary_literal)
] @number

(const_spec
  name: (identifier) @constant)

[
  (true)
  (false)
] @boolean

[
  (nil)
  (iota)
] @constant.builtin

(comment) @comment

; Go directives
((comment) @preproc
  (#match? @preproc "^//go:"))

((comment) @preproc
  (#match? @preproc "^// \\+build"))

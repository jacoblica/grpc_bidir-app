// clang-tidy manual validation file.
//
// Every identifier below is DELIBERATELY misnamed so that
// readability-identifier-naming reports a warning for it.
// Run: clang-tidy test.cpp -- -std=c++17
// Expected: 30 warnings covering all 21 configured style kinds.
//
// Style kind              Rule                       Deliberate violation
//   LocalVariable         lower_case                 LocalVariable
//   Parameter             lower_case                 Parameter / Value
//   GlobalVariable        lower_case + "g_"          GlobalVariable
//   StaticVariable        lower_case + "s_"          LocalStatic (fn-scope)
//   Member                lower_case + "_"           Member / member_value
//   Constant              UPPER_CASE                 ConstantUsed
//   ConstexprVariable     UPPER_CASE                 ConstexprVariableUsed
//   Function              camelBack                  function_used
//   Method                camelBack                  method
//   StaticMethod          camelBack                  static_method
//   VirtualMethod         camelBack                  virtual_method
//   Class                 CamelCase                  bad_class
//   Struct                CamelCase                  bad_struct
//   Union                 CamelCase                  bad_union
//   Typedef               CamelCase + "Type"         typedef_alias
//   TypeAlias             CamelCase + "Type"         type_alias
//   Enum                  CamelCase                  bad_enum
//   EnumConstant          UPPER_CASE                 bad_enum_value_a / _b
//   TypeTemplateParameter UPPER_CASE                 t
//   MacroDefinition       UPPER_CASE                 badMacroDefinition
//   Namespace             lower_case                 Bad_Namespace
//
// NOTE: with clang-tidy 18, file-scope statics (StaticVariable, line ~50) and
// static data members (StaticMemberVariable) are classified as GLOBAL
// variables, not static variables, so they are checked against the "g_" rule.

#include <cstddef>

// --- MacroDefinition ---
#define badMacroDefinition 42
#define anotherBadMacro(x) ((x) + badMacroDefinition)

// --- Namespace: expected bad_namespace ---
namespace Bad_Namespace {

// --- Typedef: expected typedef_alias_type ---
typedef unsigned long typedef_alias;

// --- TypeAlias: expected type_alias_type ---
using type_alias = int;

// --- GlobalVariable: expected g_global_variable ---
int GlobalVariable = badMacroDefinition;

// --- Enum: expected BadEnum ---
// --- EnumConstant: expected BAD_ENUM_VALUE_A / _B ---
enum bad_enum { bad_enum_value_a, bad_enum_value_b };

// --- StaticVariable at file scope: clang-tidy says global, expected g_static_variable ---
static int StaticVariable = 0;

// --- Class: expected BadClass, used bad_class ---
class bad_class {
public:
  // --- Member: expected member_, used Member ---
  int Member = 0;

  // --- Static data member: clang-tidy says global, expected g_static_member_variable ---
  static int StaticMemberVariable;

  // --- StaticMethod: expected staticMethod, used static_method ---
  static int static_method() { return StaticMemberVariable; }

  // --- VirtualMethod: expected virtualMethod, used virtual_method ---
  virtual void virtual_method();

  // --- Method: expected method, used method ---
  void method(int Parameter);
};

// --- Struct: expected BadStruct, used bad_struct ---
struct bad_struct {
  int member_value;  // expected member_value_
};

// --- Union: expected BadUnion, used bad_union ---
union bad_union {
  int int_value;
  float float_value;
};

// Definition of the static data member above.
int bad_class::StaticMemberVariable = 0;

void bad_class::method(int Parameter) {
  // --- Parameter: expected parameter ---
  // --- LocalVariable: expected local_variable ---
  int LocalVariable = Parameter;

  // --- Constant: expected CONSTANT_USED ---
  const int ConstantUsed = LocalVariable;

  // --- ConstexprVariable: expected CONSTEXPR_VARIABLE_USED ---
  constexpr int ConstexprVariableUsed = 10;

  // --- StaticVariable (function scope): expected s_local_static ---
  static int LocalStatic = 0;
  ++LocalStatic;


  Member = ConstantUsed + ConstexprVariableUsed + bad_enum_value_a +
           static_cast<int>(sizeof(type_alias)) +
           static_cast<int>(typedef_alias(0) != 0) + LocalStatic;
}

void bad_class::virtual_method() {}

// --- TypeTemplateParameter: expected T, used t ---
template <typename t>
t identity_template(t Value) {
  return Value;
}

int unused_reference() { return badMacroDefinition; }

// --- Function: expected functionUsed ---
int function_used(int parameter) {
  bad_class instance;
  bad_struct s_instance{};
  bad_union u_instance{};

  instance.method(parameter);
  instance.virtual_method();
  s_instance.member_value = parameter;
  u_instance.int_value = bad_class::static_method();
  GlobalVariable += function_used(parameter - 1);

  return identity_template(parameter) +
         static_cast<int>(sizeof(anotherBadMacro(parameter))) +
         s_instance.member_value + u_instance.int_value + unused_reference();
}

}  // namespace Bad_Namespace

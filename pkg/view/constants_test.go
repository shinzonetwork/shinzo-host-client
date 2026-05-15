package view

// Test fixtures repeated across viewManager / view / lens / schemaService tests.
const (
	// View names used in fixture View structs.
	testViewName     = "TestView"
	testViewNameOne  = "View1"
	testViewNameTwo  = "View2"
	testViewNameSame = "SameView"

	// GraphQL source-query fixtures.
	queryLogAddr       = "Log { address }"
	queryLogAddrTopics = "Log { address topics }"
	queryLogJustName   = "Log"

	queryEthLog             = "Ethereum__Mainnet__Log"
	queryEthLogAddr         = "Ethereum__Mainnet__Log { address }"
	queryEthLogAddrTopics   = "Ethereum__Mainnet__Log { address topics }"
	queryEthTransactionHash = "Ethereum__Mainnet__Transaction { hash }"

	// SDL fixtures.
	sdlTestView                = "type TestView { address: String }"
	sdlFooBase                 = "type Foo {\n  id: ID\n}"
	sdlFooMaterialized         = "type Foo @materialized(if: false) {\n  id: ID\n}"
	sdlFooMatTrue              = "type Foo @materialized(if: true) {\n  id: ID\n}"
	sdlFooBaseWithCreatedAtStr = "type Foo {\n  id: ID\n  createdAt: String\n}"

	// GraphQL scalar / field-type names used by schemaService tests.
	gqlScalarString   = "String"
	gqlScalarInt      = "Int"
	gqlScalarDateTime = "scalar DateTime"

	// SDL field metadata keys used by introspection tests.
	gqlFieldName = "name"
	gqlFieldType = "type"

	// Test fixture field name reused across schemaService_test.go cases.
	testFieldCreatedAt = "createdAt"

	// Error-message fragment expected when the view query references an
	// unprefixed source collection that the DefraDB schema resolver suggests
	// renaming to the Ethereum__Mainnet__ form.
	testErrUnknownLogCollection = `Cannot query field "Log" on type "Query". Did you mean "Ethereum__Mainnet__Log"`
)

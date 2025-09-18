package host

var getAllBlocksQuery string = `query GetAll{
  Block(limit:10){
    number
	hash
    _version{
      cid
      signature{
        type
        identity
        value
        __typename
      }
    }
  }
}
`

defmodule MyClient do
  use Boltex.Client, host: "localhost", port: 7687, user: "neo4j", password: "password"
end

defmodule Boltex.ClientTest do

  use ExUnit.Case
  alias Boltex.Node

  @moduletag :neo4j

  setup_all do
    {:ok, _} = Boltex.Server.start_link(MyClient, [])
    MyClient.connect
  end

  test "creating and returning a node with tags and properties" do
    cypher = "CREATE (x :SOME :TAG {name: 'joe', age: 22}) RETURN x"
    assert {:ok, %{"x" => node = %Node{}}} = MyClient.run(cypher)
    assert ["SOME", "TAG"] == node.tags
    assert %{"name" => "joe", "age" => 22} == node.properties
  end

end

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

  test "emptying the whole graph" do
    cypher = "MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r"
    assert :ok = MyClient.run(cypher)
  end

  test "returns error on invalid cypher statement" do
    assert {:error, _} = MyClient.run("INVALID")
    cypher = "MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r"
    assert :ok = MyClient.run(cypher)
  end

  test "returning the id of a just created node" do
    cypher = "CREATE (x) RETURN x, id(x) as id"
    assert {:ok, %{"id" => id, "x" => node}} = MyClient.run(cypher)
    assert id == node.id
  end

  test "returning a value without alias" do
    cypher = "RETURN 22"
    assert {:ok, %{"22" => 22}} = MyClient.run(cypher)
  end

end

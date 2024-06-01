defmodule TLakeTest do
  use ExUnit.Case
  doctest TLake

  test "greets the world" do
    assert TLake.hello() == :world
  end
end

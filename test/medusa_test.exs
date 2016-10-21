defmodule MedusaTest do
  use ExUnit.Case
  doctest Medusa

  defp restart_app do
    Application.stop(:medusa)
    Application.ensure_all_started(:medusa)
  end

  test "not config should fallback to default" do
    Application.delete_env(:medusa, Medusa, persistent: true)
    restart_app()
    assert Application.get_env(:medusa, Medusa) ==
      [adapter: Medusa.Adapter.Local]
  end

  test "config invalid adapter should fallback to Local" do
    Application.put_env(:medusa, Medusa, [adapter: Wrong], persistent: true)
    restart_app()
    assert Application.get_env(:medusa, Medusa) ==
      [adapter: Medusa.Adapter.Local]
  end

end

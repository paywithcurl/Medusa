defmodule MedusaTest do
  use ExUnit.Case
  doctest Medusa

  test "not config should fallback to default" do
    Application.delete_env(:medusa, Medusa, persistent: true)
    Application.stop(:medusa)
    Application.ensure_all_started(:medusa)
    assert Application.get_env(:medusa, Medusa) ==
      [adapter: Medusa.Adapter.PG2]
  end

  test "config invalid adapter should fallback to PG2" do
    Application.put_env(:medusa, Medusa, [adapter: Wrong], persistent: true)
    Application.stop(:medusa)
    Application.ensure_all_started(:medusa)
    assert Application.get_env(:medusa, Medusa) ==
      [adapter: Medusa.Adapter.PG2]
  end

  describe "Consumer" do
    test "Add consumers" do
      before = Medusa.ConsumerSupervisor |> Supervisor.which_children |> length
      Medusa.consume("foo.bob", &IO.puts/1)
      afters = Medusa.ConsumerSupervisor |> Supervisor.which_children |> length
      assert afters - before == 1
      assert Process.whereis(:"foo.bob")
    end

    test "Add invalid consumer" do
      result = Medusa.consume("foo.bob", fn -> IO.puts("blah") end)
      assert result == {:error, "arity must be 1"}
    end

    test "Add invalid consumers" do
      functions = [&IO.inspect/1, :not_a_function]
      result = Medusa.consume("foo.bob", functions)
      assert result == {:error, "arity must be 1"}
    end

    test "Add multiple consumers" do
      functions = [&IO.inspect/1, &IO.puts/1]
      result = Medusa.consume("foo.bob", functions)
      assert result == :ok
    end

    test "Only global validator and :ok" do
    end

    test "Only global validator and {:error, reason}" do
    end

    test "With 1 extra validator and both: ok" do
    end

    test "With only extra validator and :ok" do
    end

    test "With list of extra validators and all :ok" do
    end

    test "With error extra validator in the middle" do
    end

    test "With error extra validator in the end" do
    end

    test "With error extra validator in the global" do
    end
  end

  describe "Publisher" do
    test "Don't publish when validator rejects message" do
      validator = fn _, _, _ -> {:error, "failed"} end
      MedusaConfig.set_message_validator(:medusa_config, validator)
      result = Medusa.publish "validator.rejected", %{}, %{}
      MedusaConfig.set_message_validator(:medusa_config, nil)
      assert result == {:error, "message is invalid"}
    end

    test "Publish when validator accepts message" do
      validator = fn _, _, _ -> :ok end
      MedusaConfig.set_message_validator(:medusa_config, validator)
      result = Medusa.publish "validator.accepted", %{}, %{}
      MedusaConfig.set_message_validator(:medusa_config, nil)
      assert result == :ok
    end

    test "Publish adds an id in metadata if not present" do
      MedusaConfig.set_message_validator(:medusa_config, &ensures_id_present/3)
      result = Medusa.publish "validator.accepted", %{}, %{}
      MedusaConfig.set_message_validator(:medusa_config, nil)
      assert result == :ok
    end

    test "Publish leaves id in metadata if present" do
      MedusaConfig.set_message_validator(:medusa_config, &ensures_id_1234/3)
      result = Medusa.publish "validator.accepted", %{}, %{:id => 1234, :test => "blah"}
      MedusaConfig.set_message_validator(:medusa_config, nil)
      assert result == :ok
    end

    test "Publish with 1 extra validator" do
      MedusaConfig.set_message_validator(:medusa_config, &ensures_id_present/3)
      metadata = %{id: 1234}
      opts = [message_validators: &ensures_id_1234/3]
      result = Medusa.publish("validator.accepted", %{}, metadata, opts)
      assert result == :ok
    end

    test "Publish with only extra validator" do
      metadata = %{id: 1234}
      opts = [message_validators: &ensures_id_1234/3]
      result = Medusa.publish("validator.accepted", %{}, metadata, opts)
      assert result == :ok
    end

    test "Publish with list of extra validators" do
      MedusaConfig.set_message_validator(:medusa_config, &ensures_id_present/3)
      metadata = %{id: 1234}
      opts = [message_validators: [&ensures_id_1234/3, &always_ok/3]]
      result = Medusa.publish("validator.accepted", %{}, metadata, opts)
      assert result == :ok
    end

    test "Publish with error extra validator in the middle" do
      MedusaConfig.set_message_validator(:medusa_config, &ensures_id_present/3)
      metadata = %{}
      opts = [message_validators: [&ensures_id_1234/3, &always_ok/3]]
      result = Medusa.publish("validator.accepted", %{}, metadata, opts)
      assert result == {:error, "message is invalid"}
    end

    test "Publish with error extra validator in the end" do
      MedusaConfig.set_message_validator(:medusa_config, &ensures_id_present/3)
      metadata = %{}
      opts = [message_validators: [&always_ok/3, &ensures_id_1234/3]]
      result = Medusa.publish("validator.accepted", %{}, metadata, opts)
      assert result == {:error, "message is invalid"}
    end

    test "Publish with error extra validator in the global" do
      MedusaConfig.set_message_validator(:medusa_config, &ensures_id_1234/3)
      metadata = %{}
      opts = [message_validators: [&always_ok/3, &ensures_id_present/3]]
      result = Medusa.publish("validator.accepted", %{}, metadata, opts)
      assert result == {:error, "message is invalid"}
    end
  end

  defp ensures_id_1234(_, _, %{id: 1234}), do: :ok

  defp ensures_id_1234(_, _, _), do: {:error, "id not matched"}

  defp ensures_id_present(_, _, %{id: _}), do: :ok

  defp always_ok(_, _, _), do: :ok

end

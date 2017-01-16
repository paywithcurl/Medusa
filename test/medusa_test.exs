defmodule MedusaTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

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
    setup do
      Process.register(self, :self)
      :ok
    end

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
      assert capture_log(fn() ->
        functions = [&IO.inspect/1, :not_a_function]
        result = Medusa.consume("foo.bob", functions)
        assert result == {:error, "consume must be function"}
      end) =~ "consume must be function"
    end

    test "Add multiple consumers" do
      functions = [&IO.inspect/1, &IO.puts/1]
      result = Medusa.consume("foo.bob", functions)
      assert result == :ok
    end
  end

  describe "Publisher" do
    setup do
      MedusaConfig.set_message_validator(:medusa_config, nil)
      :ok
    end

    test "Don't publish when validator rejects message" do
      validator = fn _ -> {:error, "failed"} end
      MedusaConfig.set_message_validator(:medusa_config, validator)
      result = Medusa.publish "validator.rejected", %{}, %{}
      MedusaConfig.set_message_validator(:medusa_config, nil)
      assert result == {:error, "message is invalid"}
    end

    test "Publish when validator accepts message" do
      validator = fn _ -> :ok end
      MedusaConfig.set_message_validator(:medusa_config, validator)
      result = Medusa.publish "validator.accepted", %{}, %{}
      MedusaConfig.set_message_validator(:medusa_config, nil)
      assert result == :ok
    end

    test "Publish adds an id in metadata if not present" do
      MedusaConfig.set_message_validator(:medusa_config, &ensures_id_present/1)
      result = Medusa.publish "validator.accepted", %{}, %{}
      MedusaConfig.set_message_validator(:medusa_config, nil)
      assert result == :ok
    end

    test "Publish leaves id in metadata if present" do
      MedusaConfig.set_message_validator(:medusa_config, &ensures_id_1234/1)
      result = Medusa.publish "validator.accepted", %{}, %{id: 1234, test: "blah"}
      MedusaConfig.set_message_validator(:medusa_config, nil)
      assert result == :ok
    end

    test "Publish with 1 extra validator" do
      MedusaConfig.set_message_validator(:medusa_config, &ensures_id_present/1)
      metadata = %{id: 1234}
      opts = [message_validators: &ensures_id_1234/1]
      result = Medusa.publish("validator.accepted", %{}, metadata, opts)
      assert result == :ok
    end

    test "Publish with only extra validator" do
      metadata = %{id: 1234}
      opts = [message_validators: &ensures_id_1234/1]
      result = Medusa.publish("validator.accepted", %{}, metadata, opts)
      assert result == :ok
    end

    test "Publish with list of extra validators" do
      MedusaConfig.set_message_validator(:medusa_config, &ensures_id_present/1)
      metadata = %{id: 1234}
      opts = [message_validators: [&ensures_id_1234/1, &always_ok/1]]
      result = Medusa.publish("validator.accepted", %{}, metadata, opts)
      assert result == :ok
    end

    test "Publish with error extra validator in the middle" do
      MedusaConfig.set_message_validator(:medusa_config, &ensures_id_present/1)
      metadata = %{}
      opts = [message_validators: [&ensures_id_1234/1, &always_ok/1]]
      result = Medusa.publish("validator.accepted", %{}, metadata, opts)
      assert result == {:error, "message is invalid"}
    end

    test "Publish with error extra validator in the end" do
      MedusaConfig.set_message_validator(:medusa_config, &ensures_id_present/1)
      metadata = %{}
      opts = [message_validators: [&always_ok/1, &ensures_id_1234/1]]
      result = Medusa.publish("validator.accepted", %{}, metadata, opts)
      assert result == {:error, "message is invalid"}
    end

    test "Publish with error extra validator in the global" do
      MedusaConfig.set_message_validator(:medusa_config, &ensures_id_1234/1)
      metadata = %{}
      opts = [message_validators: [&always_ok/1, &ensures_id_present/1]]
      result = Medusa.publish("validator.accepted", %{}, metadata, opts)
      assert result == {:error, "message is invalid"}
    end
  end

  defp ensures_id_1234(%{metadata: %{"id" => 1234}}), do: :ok

  defp ensures_id_1234(_), do: {:error, "id not matched"}

  defp ensures_id_present(%{metadata: %{"id" => _}}), do: :ok

  defp always_ok(_), do: :ok

end

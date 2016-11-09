alias Experimental.GenStage

defmodule Medusa.Consumer.RabbitMQ do
  use GenStage
  require Logger

  def start_link(args) do
    to_link =
      args
      |> get_in([:opts, :queue_name])
      |> String.to_atom
      |> Process.whereis
    params = %{
      function: Keyword.fetch!(args, :function),
      to_link: to_link,
      opts: Keyword.get(args, :opts, [])
   }
    GenStage.start_link(__MODULE__, params)
  end

  def init(%{to_link: link} = params) do
    Logger.debug("Starting #{__MODULE__} for: #{inspect params}")
    {:consumer, params, subscribe_to: [link]}
  end

  @doc """
  Process the event passing the argument to the function.
  """
  def handle_events(events, _from, state) do
    Logger.debug("#{__MODULE__} Received event: #{inspect events}")
    events
    |> List.flatten
    |> do_handle_events(state)
  end

  def terminate(reason, state) do
    Logger.error("""
      #{__MODULE__}
      state: #{inspect state}
      die: #{inspect reason}
    """)
  end

  defp do_handle_events([], state) do
    {:noreply, [], state}
  end

  defp do_handle_events(events, %{function: f, opts: opts} = state) do
    Enum.each(events, fn event ->
      with %AMQP.Channel{} = chan <- event.metadata["channel"],
           tag when is_number(tag) <- event.metadata["delivery_tag"] do
          Task.start fn -> f.(event) end
          AMQP.Basic.ack(chan, tag)
       end
    end)
    if opts[:bind_once] do
      {:stop, :normal, state}
    else
      {:noreply, [], state}
    end
  end

end

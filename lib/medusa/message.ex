defmodule Medusa.Message do
  defstruct topic: "", body: %{}, metadata: %{}

  defmodule Info do
    defstruct channel: nil, delivery_tag: nil, retry: 0
  end
end

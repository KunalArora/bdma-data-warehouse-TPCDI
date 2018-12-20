defmodule Table do
    use Agent

    @doc """
    Starting a new Bucket
    """
    def start_link(_opts) do
        Agent.start_link(fn -> [[]] end)
    end

    @doc """
    Getting a value for a specific key
    """
    def get(agent, index) do
        Agent.get(agent, &(Enum.at(&1, index)))
    end

    def get(agent) do
        Agent.get(agent, fn store -> Enum.map(store, &(&1)) end)
    end

    @doc """
    Inserting a new Key-Value pair
    """
    def put(agent, value) do
        Agent.update(agent, &(&1++value))
    end

    @doc """
    Delete a key-value pair from the bucket
    It returns the deleted value
    """
    def delete(agent, key) do
        Agent.get_and_update(agent, fn map -> 
            Map.pop(map, key)
        end)
    end
end
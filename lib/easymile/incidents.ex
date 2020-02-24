NimbleCSV.define(CsvParser, [])

defmodule Reports.Easymile.Incidents do
  def run(params \\ []) do
    incidents = query(params)
    |> Stream.map(fn row -> for {key, val} <- row, into: %{}, do: {String.to_atom(key), val} end)
    |> Enum.into([])
    |> Enum.reduce({[], nil}, fn current, {incidents, last} ->
      if last && current.reason != last.reason && current.reason != "" do
        incident = Map.put(current, :prev_reason, last.reason)
        {[incident | incidents], current}
      else
        {incidents, current}
      end
    end)
    |> elem(0)
    |> Enum.reject(fn i -> i.reason == "button" end)

    report = incidents
    |> Enum.map(fn incident -> Map.values(incident) |> Enum.join(",") end)
    |> Enum.map(fn row -> row <> "\n" end)

    headers = incidents  |> Enum.take(1) |> Enum.map(fn incident -> Map.keys(incident) |> Enum.join(",") end) |> Enum.map(fn row -> row <> "\n" end)

    File.write!("#{__MODULE__}.csv", headers ++ report)
  end

  defp query(params \\ []) do
    Application.get_env(:prestige, :session_opts)
    |> Prestige.new_session()
    |> Prestige.stream!(statement(params))
    |> Stream.flat_map(&Prestige.Result.as_maps/1)
  end

  defp statement(params \\ []) do
    hours = Keyword.get(params, :hours, 24)
    "SELECT attributes.disengagement_reason as reason, attributes.last_seen, attributes.lat, attributes.lon, id FROM easymile__linden_states where attributes.last_seen > date_add('hour', -#{hours}, now())"
  end
end

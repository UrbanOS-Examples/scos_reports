NimbleCSV.define(CsvParser, [])

defmodule Reports.Easymile.Incidents do
  def run(params \\ []) do
    # data = query(params)
    # |> Stream.map(fn row -> for {key, val} <- row, into: %{}, do: {String.to_atom(key), val} end)
    # |> Enum.into([])

    data =
      # "#{__MODULE__}.download.csv"
      "test.csv"
      |> File.stream!(read_ahead: 100_000)
      |> CsvParser.parse_stream()
      |> Stream.map(fn [id, last_seen, lat, lon, mode, reason, speed] ->
        %{
          id: id,
          last_seen: date_parse(last_seen),
          lat: lat,
          lon: lon,
          mode: mode,
          reason: reason,
          speed: speed
        }
      end)
      |> Enum.into([])

    IO.inspect("Got data")

    incidents =
      data
      |> Enum.reduce({[], nil}, fn current, {incidents, last} ->
        if last && current.reason != last.reason && current.id == last.id do
          incident =
            current
            |> Map.put(:reason_pre, last.reason)
            |> Map.delete(:speed)

          {[incident | incidents], current}
        else
          {incidents, current}
        end
      end)
      |> elem(0)
      |> Enum.dedup()
      |> Enum.reject(fn i -> i.reason == "" end)

    IO.inspect("Got incidents")

    incidents_with_context =
      incidents
      |> Enum.map(fn incident -> get_context(incident, data) end)

    IO.inspect("Got incident contexts")

    report =
      incidents_with_context
      |> Enum.map(fn incident -> Map.values(incident) |> Enum.join(",") end)
      |> Enum.map(fn row -> row <> "\n" end)

    headers =
      incidents_with_context
      |> Enum.take(1)
      |> Enum.map(fn incident -> Map.keys(incident) |> Enum.join(",") end)
      |> Enum.map(fn row -> row <> "\n" end)

    File.write!("#{__MODULE__}.csv", headers ++ report)
  end

  defp get_context(incident, data) do
    IO.puts("Getting context for incident at: #{incident.last_seen}")
    incident_time = incident.last_seen
    incident_bound = DateTime.add(incident_time, -6, :second) |> IO.inspect(label: "incidents.ex:71")

    highest_speed_datum =
      Enum.filter(data, fn datum ->
        (incident_time > datum.last_seen) |> IO.inspect(label: "prior to incident")
        (datum.last_seen >= incident_bound) |> IO.inspect(label: "before cutoff")
        (incident_time > datum.last_seen and datum.last_seen >= incident_bound)
      end)
      |> Enum.sort(fn a, b -> a.speed > b.speed end)
      |> IO.inspect(label: "incidents.ex:79")
      |> List.first()

    if highest_speed_datum do
      %{
        disengagement_time: incident.last_seen,
        disengagement_mode: incident.mode,
        disengagement_reason: incident.reason,
        disengagement_prev_reason: incident.reason_pre,
        id: incident.id,
        lat: incident.lat,
        lon: incident.lon,
        initial_speed: highest_speed_datum.speed,
        initial_mode: highest_speed_datum.mode,
        initial_time: highest_speed_datum.last_seen
      }
    else
      %{
        disengagement_time: incident.last_seen,
        disengagement_mode: incident.mode,
        disengagement_reason: incident.reason,
        disengagement_prev_reason: incident.reason_pre,
        id: incident.id,
        lat: incident.lat,
        lon: incident.lon,
        initial_speed: nil,
        initial_mode: nil,
        initial_time: nil
      }
    end
  end

  defp date_parse(date), do: DateTime.from_iso8601(date <> "Z") |> elem(1)

  def download() do
    data =
      query()
      |> Stream.map(fn row -> for {key, val} <- row, into: %{}, do: {String.to_atom(key), val} end)
      |> Enum.into([])

    download =
      data
      |> Enum.map(fn incident -> Map.values(incident) |> Enum.join(",") end)
      |> Enum.map(fn row -> row <> "\n" end)

    headers =
      data
      |> Enum.take(1)
      |> Enum.map(fn incident -> Map.keys(incident) |> Enum.join(",") end)
      |> Enum.map(fn row -> row <> "\n" end)

    File.write!("#{__MODULE__}.download.csv", headers ++ download)
  end

  defp query(params \\ []) do
    Application.get_env(:prestige, :session_opts)
    |> Prestige.new_session()
    |> Prestige.stream!(statement(params))
    |> Stream.flat_map(&Prestige.Result.as_maps/1)
  end

  defp statement(params \\ []) do
    hours = Keyword.get(params, :hours, 24)

    "SELECT attributes.disengagement_reason as reason, attributes.last_seen, attributes.lat, attributes.lon, attributes.speed, attributes.mode, id FROM easymile__linden_states order by id desc, attributes.last_seen asc"
  end
end

import { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";

type Meta = {
  symbol: string;
  min_date: string | null;
  max_date: string | null;
};

type HistoryPoint = { ts: string; close: number };

type RangePreset = "1M" | "3M" | "6M" | "1Y" | "5Y";
type Granularity = "day" | "week" | "month";

function isoDate(d: Date) {
  // YYYY-MM-DD
  return d.toISOString().slice(0, 10);
}

function subtractPreset(anchor: Date, preset: RangePreset) {
  const d = new Date(anchor);
  if (preset === "1M") d.setMonth(d.getMonth() - 1);
  if (preset === "3M") d.setMonth(d.getMonth() - 3);
  if (preset === "6M") d.setMonth(d.getMonth() - 6);
  if (preset === "1Y") d.setFullYear(d.getFullYear() - 1);
  if (preset === "5Y") d.setFullYear(d.getFullYear() - 5);
  return d;
}

export default function HomePage() {
  const [symbols, setSymbols] = useState<string[]>([]);
  const [symbol, setSymbol] = useState<string>("AAPL");

  const [preset, setPreset] = useState<RangePreset>("1Y");
  const [granularity, setGranularity] = useState<Granularity>("week");

  const [maxDate, setMaxDate] = useState<Date | null>(null);
  const [minDate, setMinDate] = useState<Date | null>(null);

  const [data, setData] = useState<HistoryPoint[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  // Load symbols for dropdown
  useEffect(() => {
    fetch("/api/finance/symbols")
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((json) => {
        const list = (json.data as string[]) ?? [];
        setSymbols(list);

        // Keep current symbol if it's valid; otherwise pick the first available.
        if (list.length && !list.includes(symbol)) setSymbol(list[0] ?? "AAPL");
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Load meta (min/max available date) whenever the symbol changes.
  useEffect(() => {
    if (!symbol) return;

    setError(null);
    setMaxDate(null);
    setMinDate(null);

    fetch(`/api/finance/meta?symbol=${encodeURIComponent(symbol)}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((json: Meta) => {
        setMaxDate(json.max_date ? new Date(json.max_date) : null);
        setMinDate(json.min_date ? new Date(json.min_date) : null);
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, [symbol]);

  // Compute the requested window *relative to the latest available date* for the symbol.
  const { start, end, anchorLabel } = useMemo(() => {
    const anchor = maxDate ?? new Date(); // fallback while meta loads
    const s = subtractPreset(anchor, preset);

    // End is exclusive in the API; add 1 day to include the anchor day.
    const e = new Date(anchor);
    e.setDate(e.getDate() + 1);

    return {
      start: isoDate(s),
      end: isoDate(e),
      anchorLabel: isoDate(anchor),
    };
  }, [preset, maxDate]);

  // Load history whenever controls change (but wait until meta has loaded).
  useEffect(() => {
    if (!symbol) return;
    if (!maxDate) return; // important: avoid querying relative to "now" before meta is ready

    setLoading(true);
    setError(null);
    setData(null);

    const params = new URLSearchParams({
      symbol,
      start,
      end,
      granularity,
    });

    fetch(`/api/finance/history?${params.toString()}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((json) => setData((json.data as HistoryPoint[]) ?? []))
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false));
  }, [symbol, start, end, granularity, maxDate]);

  const priceFormatter = useMemo(
    () =>
      new Intl.NumberFormat(undefined, {
        style: "currency",
        currency: "USD",
        maximumFractionDigits: 2,
      }),
    []
  );

  return (
    <main className="mx-auto max-w-5xl px-6 py-10 space-y-6">
      <Card>
        <CardHeader className="space-y-4">
          <CardTitle>Stock Explorer</CardTitle>

          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            {/* Left controls */}
            <div className="flex flex-wrap items-center gap-3">
              {/* Symbol */}
              <div className="w-[180px]">
                <Select value={symbol} onValueChange={setSymbol}>
                  <SelectTrigger>
                    <SelectValue placeholder="Select symbol" />
                  </SelectTrigger>
                  <SelectContent>
                    {symbols.map((s) => (
                      <SelectItem key={s} value={s}>
                        {s}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {/* Granularity */}
              <div className="w-[160px]">
                <Select
                  value={granularity}
                  onValueChange={(v) => setGranularity(v as Granularity)}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Granularity" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="day">Daily</SelectItem>
                    <SelectItem value="week">Weekly</SelectItem>
                    <SelectItem value="month">Monthly</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            {/* Right controls: range presets */}
            <div className="flex flex-wrap gap-2">
              {(["1M", "3M", "6M", "1Y", "5Y"] as RangePreset[]).map((p) => (
                <Button
                  key={p}
                  variant={preset === p ? "default" : "outline"}
                  size="sm"
                  onClick={() => setPreset(p)}
                >
                  {p}
                </Button>
              ))}
            </div>
          </div>

          <div className="text-sm text-muted-foreground">
            Showing {symbol} from <span className="font-medium">{start}</span>{" "}
            to <span className="font-medium">{end}</span> (bucketed{" "}
            <span className="font-medium">{granularity}</span>
            {maxDate && (
              <>
                , latest available:{" "}
                <span className="font-medium">{anchorLabel}</span>
              </>
            )}
            {minDate && (
              <>
                , earliest:{" "}
                <span className="font-medium">{isoDate(minDate)}</span>
              </>
            )}
            )
          </div>
        </CardHeader>

        <CardContent>
          {error && <div className="text-sm text-red-500">Error: {error}</div>}

          {!maxDate && !error && (
            <div className="text-sm text-muted-foreground">
              Loading metadata…
            </div>
          )}

          {loading && (
            <div className="text-sm text-muted-foreground">Loading chart…</div>
          )}

          {!loading && data && data.length === 0 && (
            <div className="text-sm text-muted-foreground">
              No data returned for this selection.
            </div>
          )}

          {data && data.length > 0 && (
            <div className="h-[360px] w-full">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={data}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis
                    dataKey="ts"
                    tickFormatter={(v) => String(v).slice(0, 10)}
                    minTickGap={24}
                  />
                  <YAxis width={60} domain={["auto", "auto"]} />
                  <Tooltip
                    formatter={(value) => priceFormatter.format(Number(value))}
                    labelFormatter={(label) =>
                      `Date: ${String(label).slice(0, 10)}`
                    }
                  />
                  <Line
                    type="monotone"
                    dataKey="close"
                    dot={false}
                    strokeWidth={2}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}
        </CardContent>
      </Card>
    </main>
  );
}

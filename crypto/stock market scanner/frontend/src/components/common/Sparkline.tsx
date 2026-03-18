/**
 * Sparkline — tiny inline chart for the scanner table.
 * Uses Recharts <LineChart> with no axes, grid, or labels.
 */

import { memo } from 'react';
import { LineChart, Line, YAxis } from 'recharts';

interface Props {
  data: number[];
  width?: number;
  height?: number;
  positive?: boolean;
}

export const Sparkline = memo(function Sparkline({
  data,
  width = 80,
  height = 28,
  positive,
}: Props) {
  if (!data || data.length < 2) return <span className="text-zinc-600">—</span>;

  const color = positive === undefined
    ? '#6366f1'
    : positive ? '#22c55e' : '#ef4444';

  const points = data.map((v, i) => ({ v, i }));

  return (
    <LineChart width={width} height={height} data={points}>
      <YAxis domain={['dataMin', 'dataMax']} hide />
      <Line
        type="monotone"
        dataKey="v"
        stroke={color}
        strokeWidth={1.5}
        dot={false}
        isAnimationActive={false}
      />
    </LineChart>
  );
});

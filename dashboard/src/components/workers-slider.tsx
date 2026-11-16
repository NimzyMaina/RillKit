import { Input } from "@/components/ui/input";
import { Slider } from "@/components/ui/slider";

export const MAX_WORKERS = 256;

/** A slider for 1..64 lanes, with an exact number box that goes up to 256. */
export function WorkersSlider({
  id,
  value,
  onChange,
  disabled,
}: {
  id: string;
  value: number;
  onChange: (n: number) => void;
  disabled?: boolean;
}) {
  const sliderMax = value > 64 ? MAX_WORKERS : 64;
  return (
    <div className="flex items-center gap-4">
      <Slider
        aria-label="Workers"
        min={1}
        max={sliderMax}
        step={1}
        value={[value]}
        onValueChange={([v]) => onChange(v)}
        disabled={disabled}
        className="flex-1"
      />
      <Input
        id={id}
        type="number"
        inputMode="numeric"
        min={1}
        max={MAX_WORKERS}
        value={value}
        disabled={disabled}
        onChange={(e) => {
          const n = Math.round(Number(e.target.value));
          if (Number.isFinite(n)) onChange(Math.min(MAX_WORKERS, Math.max(1, n)));
        }}
        className="w-20 text-center font-mono font-numeric"
      />
    </div>
  );
}

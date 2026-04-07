import { useState } from "react";
import { clsx } from "clsx";
import { getAssetFallbackColor, getAssetIconUrl } from "@/lib/assetIcons";

type Props = {
  symbol: string;
  size?: number;
  className?: string;
  textClassName?: string;
};

export default function AssetAvatar({
  symbol,
  size = 32,
  className,
  textClassName,
}: Props) {
  const name = symbol.toUpperCase();
  const [failed, setFailed] = useState(false);
  const url = getAssetIconUrl(name);
  const showImg = url && !failed;
  const bg = getAssetFallbackColor(name);
  const initials = name.slice(0, 2);

  return (
    <div
      className={clsx(
        "rounded-full flex items-center justify-center shrink-0 overflow-hidden",
        className,
      )}
      style={{ width: size, height: size, backgroundColor: showImg ? "transparent" : bg }}
    >
      {showImg ? (
        <img
          src={url}
          alt=""
          className="w-full h-full object-cover"
          onError={() => setFailed(true)}
        />
      ) : (
        <span
          className={clsx("font-bold text-white leading-none", textClassName)}
          style={{ fontSize: Math.max(10, size * 0.35) }}
        >
          {initials}
        </span>
      )}
    </div>
  );
}

// Hand-rolled stroke icons (no icon dependency). 1.6px stroke, inherit color.
const base = {
  width: 18,
  height: 18,
  viewBox: "0 0 24 24",
  fill: "none",
  stroke: "currentColor",
  strokeWidth: 1.7,
  strokeLinecap: "round",
  strokeLinejoin: "round",
};

export const IconOverview = (p) => (
  <svg {...base} {...p}>
    <rect x="3" y="3" width="7" height="9" rx="1" />
    <rect x="14" y="3" width="7" height="5" rx="1" />
    <rect x="14" y="12" width="7" height="9" rx="1" />
    <rect x="3" y="16" width="7" height="5" rx="1" />
  </svg>
);

export const IconLane = (p) => (
  <svg {...base} {...p}>
    <path d="M4 21 8 3" />
    <path d="M20 21 16 3" />
    <path d="M12 5v2M12 11v2M12 17v2" />
  </svg>
);

export const IconCamera = (p) => (
  <svg {...base} {...p}>
    <path d="M3 8h3l1.5-2h9L18 8h3v11H3z" />
    <circle cx="12" cy="13" r="3.2" />
  </svg>
);

export const IconCar = (p) => (
  <svg {...base} {...p}>
    <path d="M3 13l2-5a2 2 0 0 1 1.9-1.3h10.2A2 2 0 0 1 19 8l2 5" />
    <path d="M3 13h18v4H3z" />
    <circle cx="7" cy="17.5" r="1.4" />
    <circle cx="17" cy="17.5" r="1.4" />
  </svg>
);

export const IconViolations = (p) => (
  <svg {...base} {...p}>
    <path d="M12 3 2.5 19.5h19z" />
    <path d="M12 10v4" />
    <circle cx="12" cy="17" r="0.6" fill="currentColor" />
  </svg>
);

export const IconPlus = (p) => (
  <svg {...base} {...p}>
    <path d="M12 5v14M5 12h14" />
  </svg>
);

export const IconDownload = (p) => (
  <svg {...base} {...p}>
    <path d="M12 3v12" />
    <path d="m7 11 5 5 5-5" />
    <path d="M4 21h16" />
  </svg>
);

export const IconSearch = (p) => (
  <svg {...base} {...p}>
    <circle cx="11" cy="11" r="7" />
    <path d="m21 21-4.3-4.3" />
  </svg>
);

export const IconChevron = (p) => (
  <svg {...base} {...p}>
    <path d="m9 6 6 6-6 6" />
  </svg>
);

export const IconArrowRight = (p) => (
  <svg {...base} {...p}>
    <path d="M5 12h14" />
    <path d="m13 6 6 6-6 6" />
  </svg>
);

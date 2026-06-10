import React from "react";
import ReactDOM from "react-dom/client";
import { createBrowserRouter, RouterProvider, Navigate } from "react-router-dom";

import AppShell from "./components/AppShell.jsx";
import Overview from "./pages/Overview.jsx";
import LaneDashboard from "./pages/LaneDashboard.jsx";
import Cameras from "./pages/Cameras.jsx";
import Cars from "./pages/Cars.jsx";
import Violations from "./pages/Violations.jsx";
import "./index.css";

const router = createBrowserRouter([
  {
    path: "/",
    element: <AppShell />,
    children: [
      { index: true, element: <Overview /> },
      { path: "lanes/:laneId", element: <LaneDashboard /> },
      { path: "cameras", element: <Cameras /> },
      { path: "cars", element: <Cars /> },
      { path: "violations", element: <Violations /> },
      { path: "*", element: <Navigate to="/" replace /> },
    ],
  },
]);

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <RouterProvider router={router} />
  </React.StrictMode>
);

import { Link, Route, Routes } from "react-router-dom";
import { Layout } from "./components/Layout.jsx";
import { Overview } from "./pages/Overview.jsx";

export const ROUTES = [{ path: "/", element: <Overview /> }];

function NotFound() {
  return (
    <div className="card empty">
      <h2>Nothing here</h2>
      <p>
        That address is not part of the dashboard. <Link to="/">Back to the overview</Link>.
      </p>
    </div>
  );
}

export function App() {
  return (
    <Routes>
      <Route element={<Layout routes={ROUTES.map((r) => r.path)} />}>
        {ROUTES.map((route) =>
          route.path === "/" ? (
            <Route key="index" index element={route.element} />
          ) : (
            <Route key={route.path} path={route.path} element={route.element} />
          ),
        )}
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}

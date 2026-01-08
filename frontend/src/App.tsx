import { createBrowserRouter, RouterProvider } from "react-router-dom";
import RootLayout from "@/layouts/root-layout";
import HomePage from "@/pages/home";
import DocsPage from "@/pages/docs";
import AboutPage from "@/pages/about";

const router = createBrowserRouter([
  {
    path: "/",
    element: <RootLayout />,
    children: [
      { index: true, element: <HomePage /> },
      { path: "docs", element: <DocsPage /> },
      { path: "about", element: <AboutPage /> },
    ],
  },
]);

export default function App() {
  return <RouterProvider router={router} />;
}

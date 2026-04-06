import { Component, type ErrorInfo, type ReactNode } from "react";

interface Props {
  children: ReactNode;
}

interface State {
  hasError: boolean;
  message: string;
}

export default class ErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false, message: "" };

  static getDerivedStateFromError(err: Error): State {
    return { hasError: true, message: err.message || "Something went wrong" };
  }

  componentDidCatch(err: Error, info: ErrorInfo) {
    console.error("Mini App error boundary:", err, info.componentStack);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="flex-1 flex flex-col items-center justify-center bg-tg-bg px-6 py-10">
          <p className="text-tg-text text-lg font-semibold mb-2">Something broke</p>
          <p className="text-tg-hint text-sm text-center mb-6">{this.state.message}</p>
          <button
            type="button"
            onClick={() => this.setState({ hasError: false, message: "" })}
            className="px-6 py-2 rounded-xl bg-tg-button text-tg-button-text text-sm font-medium"
          >
            Try again
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

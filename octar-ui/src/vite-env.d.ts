// Global window extensions
declare global {
  interface Window {
    d3: typeof import('d3');
    webix: typeof import('webix');
    OctarUI: {
      createVisualization: (container: HTMLElement, options: any) => void;
    };
  }
}

export {};
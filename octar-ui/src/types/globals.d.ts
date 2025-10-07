// Global type declarations for window globals

declare global {
  interface Window {
    OctarUI: {
      createVisualization: (container: HTMLElement) => {
        svg: any;
        nodes: any[];
        edges: any[];
      };
    };
  }
}

export {};
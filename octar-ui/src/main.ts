import './style.css'
import { 
  DEMO_NODES, 
  DEMO_EDGES,
  createSVGCanvas,
  setupArrowMarkers,
  renderEdges,
  renderActorNodes,
  addTitleText
} from './ctx/foundation';

// Main demo visualization function
export function createVisualization(container: HTMLElement) {
  // Create SVG canvas
  const svg = createSVGCanvas(container);
  
  // Setup arrow markers for edges
  setupArrowMarkers(svg);
  
  // Render edges first (so they appear behind nodes)
  renderEdges(svg, DEMO_EDGES);
  
  // Render actor nodes with Webix widgets
  renderActorNodes(svg, DEMO_NODES);
  
  // Add title text
  addTitleText(svg);
  
  console.log('POC initialized successfully!');
  
  return { svg, nodes: DEMO_NODES, edges: DEMO_EDGES };
}

// Attach to window for global access
window.OctarUI = {
  createVisualization
};

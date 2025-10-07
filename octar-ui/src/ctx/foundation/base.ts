// Foundation context - base types and data structures for the demo

export interface ActorNode {
  id: string;
  name: string;
  x: number;
  y: number;
  count: number;
}

export interface ActorEdge {
  source: ActorNode;
  target: ActorNode;
}

export const DEMO_NODES: ActorNode[] = [
  { id: 'actor1', name: 'Counter Actor', x: 150, y: 100, count: 0 },
  { id: 'actor2', name: 'Producer Actor', x: 400, y: 100, count: 5 },
  { id: 'actor3', name: 'Consumer Actor', x: 275, y: 250, count: 10 }
];

export const DEMO_EDGES: ActorEdge[] = [
  { source: DEMO_NODES[0], target: DEMO_NODES[1] },
  { source: DEMO_NODES[1], target: DEMO_NODES[2] }
];
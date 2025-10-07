// Core rendering functions using D3 and Webix
import type { ActorNode, ActorEdge } from './base';
import * as d3 from 'd3';
import webix from 'webix';

// Type alias for D3 selection
type SVGSelection = d3.Selection<SVGSVGElement, unknown, null, undefined>;

export function createSVGCanvas(container: HTMLElement): SVGSelection {
    return d3.select(container)
        .append('svg')
        .attr('width', '100%')
        .attr('height', '100%')
        .style('background', '#fafafa');
}

export function setupArrowMarkers(svg: SVGSelection) {
    svg.append('defs').append('marker')
        .attr('id', 'arrowhead')
        .attr('viewBox', '-0 -5 10 10')
        .attr('refX', 13)
        .attr('refY', 0)
        .attr('orient', 'auto')
        .attr('markerWidth', 13)
        .attr('markerHeight', 13)
        .attr('xoverflow', 'visible')
        .append('svg:path')
        .attr('d', 'M 0,-5 L 10 ,0 L 0,5')
        .attr('fill', '#999')
        .style('stroke', 'none');
}

export function renderEdges(svg: SVGSelection, edges: ActorEdge[]) {
    svg.selectAll('.edge')
        .data(edges)
        .enter()
        .append('line')
        .attr('class', 'edge')
        .attr('x1', (d: ActorEdge) => d.source.x + 79) // Widget center (158/2)
        .attr('y1', (d: ActorEdge) => d.source.y + 44) // Widget center (88/2)
        .attr('x2', (d: ActorEdge) => d.target.x + 79)
        .attr('y2', (d: ActorEdge) => d.target.y + 44)
        .attr('stroke', '#666')
        .attr('stroke-width', 2)
        .attr('stroke-dasharray', '5,5')
        .attr('marker-end', 'url(#arrowhead)');
}

export function renderActorNodes(svg: SVGSelection, nodes: ActorNode[]) {
    // Create nodes group
    const nodeGroup = svg.selectAll('.node')
        .data(nodes)
        .enter()
        .append('g')
        .attr('class', 'node')
        .attr('transform', (d: ActorNode) => `translate(${d.x}, ${d.y})`);

    // Create foreignObject elements for Webix widgets
    const foreignObjects = nodeGroup.append('foreignObject')
        .attr('x', 0)
        .attr('y', 0)
        .attr('width', 158)
        .attr('height', 88);

    // Create outer containers
    const outerContainers = foreignObjects.append('xhtml:div')
        .attr('class', 'node-container');

    // Create inner div containers for Webix widgets
    const widgetContainers = outerContainers.append('div')
        .attr('class', 'node-widget');

    // Initialize Webix widgets
    nodes.forEach((nodeData, index) => {
        const containerId = `widget-${nodeData.id}`;

        // Set ID on the container
        widgetContainers.filter((_d: unknown, i: number) => i === index)
            .attr('id', containerId);

        createWebixWidget(containerId, nodeData);
    });
}

function createWebixWidget(containerId: string, nodeData: ActorNode) {
    webix.ui({
        container: containerId,
        rows: [
            {
                view: "label",
                label: nodeData.name,
                css: "webix_header",
                height: 30,
                align: "center"
            },
            {
                cols: [
                    {
                        view: "label",
                        label: `Count: ${nodeData.count}`,
                        id: `count-${nodeData.id}`,
                        align: "center"
                    },
                    {
                        view: "button",
                        value: "+1",
                        width: 35,
                        css: "webix_primary",
                        click: function () {
                            // Update the data
                            nodeData.count++;

                            // Update the label
                            const countLabel = webix.$$(`count-${nodeData.id}`) as webix.ui.label;
                            if (countLabel) {
                                countLabel.define("label", `Count: ${nodeData.count}`);
                                countLabel.refresh();
                            }

                            console.log(`${nodeData.name} count updated to: ${nodeData.count}`);


                            // Visual feedback
                            const widget = d3.select(`#${containerId}`)
                                .select('.node-widget');
                            widget.style('border-color', '#FF5722')
                                .transition()
                                .duration(500)
                                .style('border-color', '#4CAF50');
                        }
                    }
                ]
            }
        ]
    });
}

export function addTitleText(svg: SVGSelection) {
    svg.append('text')
        .attr('x', 20)
        .attr('y', 30)
        .style('font-size', '14px')
        .style('font-weight', 'bold')
        .text('Actor System Visualization POC');

    svg.append('text')
        .attr('x', 20)
        .attr('y', 50)
        .style('font-size', '12px')
        .text('Click the +1 buttons to test Webix widget interaction');
}
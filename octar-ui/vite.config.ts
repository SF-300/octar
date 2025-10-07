import { defineConfig } from 'vite'
// @ts-ignore - plugin types might not be available
import externalGlobals from 'vite-plugin-external-globals'

export default defineConfig({
  plugins: [
    // Transform imports to global references during build
    externalGlobals({
      entry: [
        {
          name: 'd3',
          var: 'd3'
        },
        {
          name: 'webix',
          var: 'webix'
        }
      ]
    })
  ],
  
  // Development server configuration
  server: {
    open: true, // Open browser automatically
    port: 3000
  },
  
  build: {
    lib: {
      entry: 'src/main.ts',
      name: 'OctarUI',
      fileName: (format) => `octar-ui.${format}.js`,
      formats: ['umd'] // UMD for browser script tags
    },
    rollupOptions: {
      external: ['d3', 'webix'], // Externalize peer dependencies
      output: {
        globals: {
          // Map module imports to global variable names
          'd3': 'd3',        // import * as d3 from 'd3' -> window.d3
          'webix': 'webix'   // import webix from 'webix' -> window.webix
        }
      }
    },
    sourcemap: true, // For debugging
    target: 'es2020' // Modern browsers
  }
})
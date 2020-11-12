import json from '@rollup/plugin-json';
import bundleSize from 'rollup-plugin-bundle-size';
import { nodeResolve } from '@rollup/plugin-node-resolve';
import { terser } from 'rollup-plugin-terser';

function onwarn(warning, defaultHandler) {
  if (warning.code !== 'CIRCULAR_DEPENDENCY') {
    defaultHandler(warning);
  }
}

const buildQuery = {
  input: 'src/index.js',
  plugins: [
    json(),
    bundleSize(),
    nodeResolve({ modulesOnly: true })
  ],
  onwarn,
  output: [
    {
      file: 'dist/arquero-worker-client.js',
      name: 'aq',
      format: 'umd'
    },
    {
      file: 'dist/arquero-worker-client.min.js',
      name: 'aq',
      format: 'umd',
      sourcemap: true,
      plugins: [ terser({ ecma: 2018 }) ]
    },
    {
      file: 'dist/arquero-worker-client.mjs',
      format: 'es'
    }
  ]
};

const buildWorker = {
  input: 'src/worker/index.js',
  plugins: [
    json(),
    bundleSize(),
    nodeResolve({ modulesOnly: true })
  ],
  onwarn,
  output: [
    {
      file: 'dist/arquero-worker.js',
      format: 'iife'
    },
    {
      file: 'dist/arquero-worker.min.js',
      name: 'aq',
      format: 'iife',
      sourcemap: true,
      plugins: [ terser({ ecma: 2018 }) ]
    }
  ]
};

const buildNodeWorker = {
  input: 'src/worker/index-node.js',
  plugins: [
    json(),
    bundleSize(),
    nodeResolve({ modulesOnly: true })
  ],
  onwarn,
  output: [
    {
      file: 'dist/arquero-node-worker.js',
      format: 'cjs'
    }
  ]
};

/**
 * Command line arguments:
 * - node-worker
 */
export default function(args) {
  const nodeWorker = !!args['config-node-worker'];
  return nodeWorker ? buildNodeWorker
    : [buildQuery, buildWorker, buildNodeWorker];
}
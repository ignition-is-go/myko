import { resolve } from 'path';
import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';
import dts from 'vite-plugin-dts';

export default defineConfig({
	plugins: [vue(), dts()],
	build: {
		lib: {
			entry: resolve(__dirname, 'src/lib/index.ts'),
			formats: ['es', 'cjs'],
			fileName: (format) => `index.${format === 'es' ? 'mjs' : 'cjs'}`,
		},
		rollupOptions: {
			external: ['vue', '@myko/core', 'rxjs', 'luxon'],
		},
	},
});

import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';
import dts from 'vite-plugin-dts';
import { resolve } from 'path';

export default defineConfig({
	plugins: [
		vue(),
		dts({
			include: ['src/lib/**/*.ts'],
			outDir: 'dist'
		})
	],
	build: {
		lib: {
			entry: {
				index: resolve(__dirname, 'src/lib/index.ts'),
				client: resolve(__dirname, 'src/lib/services/client-exports.ts')
			},
			formats: ['es']
		},
		rollupOptions: {
			external: ['vue', '@myko/ts', 'rxjs'],
			output: {
				preserveModules: false
			}
		},
		outDir: 'dist',
		emptyOutDir: true
	}
});

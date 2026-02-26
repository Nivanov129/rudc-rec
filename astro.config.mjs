import { defineConfig } from 'astro/config';
import tailwindcss from '@tailwindcss/vite';

export default defineConfig({
  site: 'https://nivanov129.github.io',
  base: '/rudc-rec',
  vite: {
    plugins: [tailwindcss()]
  }
});

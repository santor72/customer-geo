export default defineNuxtConfig({
  ssr: false,
  modules: ['@nuxtjs/tailwindcss'],
  css: ['~/assets/css/main.css'],
  devtools: { enabled: false },
  nitro: {
    prerender: {
      crawlLinks: false,
      routes: ['/'],
    },
  },
})

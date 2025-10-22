/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    './app/**/*.{js,ts,jsx,tsx}',
    './components/**/*.{js,ts,jsx,tsx}',
  ],
  theme: {
    extend: {
      colors: {
        bg: '#f5f6f8',
        card: '#ffffff',
        primary: '#111827',
        muted: '#6b7280',
        accent: '#a3e635',
      },
      boxShadow: {
        card: '0 10px 25px -15px rgba(0,0,0,0.2)'
      },
      borderRadius: {
        xl: '1rem'
      }
    },
  },
  plugins: [],
}

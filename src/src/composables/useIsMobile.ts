import { ref } from 'vue'

const MOBILE_BREAKPOINT = 768

const isMobile = ref(
  typeof window !== 'undefined' ? window.innerWidth < MOBILE_BREAKPOINT : false,
)

if (typeof window !== 'undefined') {
  window.addEventListener('resize', () => {
    isMobile.value = window.innerWidth < MOBILE_BREAKPOINT
  })
}

export function useIsMobile() {
  return { isMobile }
}

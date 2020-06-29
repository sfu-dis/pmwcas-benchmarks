
#ifdef _DEBUG
#define verify(exp) assert(exp)
#else
#define verify(exp) ((void)0)
#endif

#define MARK_UNREFERENCED(P) ((void)P)

#define PREFETCH_KEY_DATA(key) _mm_prefetch(key.data(), _MM_HINT_T0)
#define PREFETCH_NEXT_PAGE(delta) _mm_prefetch((char*)(delta->next_page), _MM_HINT_T0)

// Returns true if \a x is a power of two.
#define IS_POWER_OF_TWO(x) (x && (x & (x - 1)) == 0)

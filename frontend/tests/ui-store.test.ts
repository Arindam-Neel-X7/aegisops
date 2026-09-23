import { useUIStore } from '@/stores/ui-store';

describe('UI Store', () => {
  beforeEach(() => {
    // Reset state before each test if necessary, though testing pure state changes
    useUIStore.setState({ isSidebarOpen: true });
  });

  it('should have sidebar open by default', () => {
    const state = useUIStore.getState();
    expect(state.isSidebarOpen).toBe(true);
  });

  it('should toggle sidebar state', () => {
    const state = useUIStore.getState();
    state.toggleSidebar();
    
    expect(useUIStore.getState().isSidebarOpen).toBe(false);
    
    useUIStore.getState().toggleSidebar();
    expect(useUIStore.getState().isSidebarOpen).toBe(true);
  });

  it('should explicitly set sidebar state', () => {
    const state = useUIStore.getState();
    state.setSidebarOpen(false);
    expect(useUIStore.getState().isSidebarOpen).toBe(false);
    
    state.setSidebarOpen(true);
    expect(useUIStore.getState().isSidebarOpen).toBe(true);
  });
});

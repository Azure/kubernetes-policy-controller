package constraintstatus



//func TestTotalConstraintsCache(t *testing.T) {
//	constraintsCache := NewConstraintsCache()
//	if len(constraintsCache.cache) != 0 {
//		t.Errorf("cache: %v, wanted empty cache", spew.Sdump(constraintsCache.cache))
//	}
//
//	constraintsCache.addConstraintKey("test", tags{
//		enforcementAction: util.Deny,
//		status:            metrics.ActiveStatus,
//	})
//	if len(constraintsCache.cache) != 1 {
//		t.Errorf("cache: %v, wanted cache with 1 element", spew.Sdump(constraintsCache.cache))
//	}
//
//	constraintsCache.deleteConstraintKey("test")
//	if len(constraintsCache.cache) != 0 {
//		t.Errorf("cache: %v, wanted empty cache", spew.Sdump(constraintsCache.cache))
//	}
//}

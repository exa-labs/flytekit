#!/usr/bin/env python3
"""
Test script to verify flytekit imports work after protobuf fix.
"""

try:
    print("Testing flytekit import...")
    from flytekit import task, workflow
    print("✅ flytekit import successful - RegisterExtension issue resolved!")
    
    @task
    def test_task() -> str:
        return "Hello from fixed flytekit!"
    
    @workflow  
    def test_workflow() -> str:
        return test_task()
    
    print("✅ Task and workflow creation successful!")
    
    result = test_workflow()
    print(f"✅ Workflow execution successful: {result}")
    
except Exception as e:
    print(f"❌ Import/execution still failing: {e}")
    import traceback
    traceback.print_exc()

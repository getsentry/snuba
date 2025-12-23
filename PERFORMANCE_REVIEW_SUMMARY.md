# SE2 Performance Review Summary

## Overview
This document summarizes key contributions to the Snuba project during the review period, demonstrating ownership of projects, technical execution, and cross-functional collaboration on the Capacity-Based Routing System (CBRS) and Events Analytics Platform (EAP).

---

## Major Project: Capacity-Based Routing System (CBRS)

### Project Context
Led the development and implementation of the Capacity-Based Routing System, a medium-complexity project that enables intelligent routing of queries to different ClickHouse clusters based on load, performance metrics, and configurable policies. This system is critical for optimizing resource utilization and preventing cluster overload in Sentry's infrastructure.

---

## Technical Summary: What These PRs Actually Built

### Core System Architecture
Built a complete capacity-based routing system that intelligently routes Snuba queries to different ClickHouse clusters based on real-time load metrics and configurable policies. The system consists of:

**1. Cluster Load Monitoring (PR #7143)**
- Implemented mechanism to query ClickHouse system tables to retrieve current cluster load metrics
- Built aggregation logic to calculate load statistics for EAP item clusters
- Created data structures to represent cluster health and capacity information

**2. Routing Strategy Framework (PR #7201, #7337)**
- Migrated routing logic from internal Snuba layers to the RPC layer for better separation of concerns
- Unified `AllocationPolicy` and `RoutingStrategy` under a common `ConfigurableComponent` abstraction
- Enabled dynamic component instantiation by namespace and class name
- Created a flexible configuration system where routing behavior can be modified without code changes

**3. Component Registry System (PR #7379, #7401)**
- Built `RegisteredClass` metaclass integration for automatic component discovery
- Implemented namespace-based component isolation (e.g., `AllocationPolicy.BytesScannedWindowPolicy`)
- Fixed registry pollution bug where subclasses across different namespaces shared the same registry
- Created dual API: `ConfigurableComponent.get_component_class(namespace).get_from_name(name)` and `AllocationPolicy.get_from_name(name)`

**4. Allocation Policy Enhancements (PR #7375, #7411)**
- Integrated resource identifiers into allocation policies (moving beyond just storage keys)
- Made allocation policies self-aware of their type (select vs. delete policies)
- Migrated EAP-specific allocation policies from generic layer into the outcomes-based routing strategy
- Enabled routing strategies to encapsulate their own allocation policies for better cohesion

**5. Configuration Management APIs (PR #7346)**
- Built RESTful endpoints in Snuba Admin for:
  - Listing available routing strategies and allocation policies
  - Retrieving current configuration for a resource
  - Updating routing strategy configuration with validation
- Implemented payload serialization/deserialization for `ConfigurableComponent` instances
- Added dependency coordination between backend refactoring PRs

**6. User Interface (PR #7399)**
- Developed Snuba Admin UI page for CBRS configuration
- Built forms for selecting routing strategies and setting configuration parameters
- Integrated with backend APIs for reading/writing configurations
- Added visual feedback for configuration changes and validation errors

**7. API Refinements (PR #7421)**
- Removed confusing default routing decision parameter from API
- Simplified routing strategy interface to only track necessary routing context
- Cleaned up fail-open logic to reduce cognitive complexity

**8. Production Quality Fixes (PR #7241, #7473)**
- Fixed missing request metadata in routing decision objects causing NoneType errors
- Corrected querylog referrer field that was incorrectly logging routing strategy name instead of actual referrer
- Both fixes improved observability and prevented runtime errors in production

### Technical Design Patterns Used

**Configurable Component Pattern**: Created an abstract base class with metaclass magic that allows:
- Automatic registration of subclasses in a global registry
- Dynamic instantiation by string name
- Namespace isolation to prevent class name collisions
- Serialization/deserialization for API transport

**Strategy Pattern**: Routing strategies encapsulate different algorithms for cluster selection:
- `OutcomesBasedRoutingStrategy`: Routes based on historical query outcomes
- Future strategies can be added without modifying core routing logic

**Policy Composition**: Allocation policies are composable filters that can:
- Reject queries based on bytes scanned thresholds
- Enforce concurrent query limits per resource
- Implement guardrails for specific referrers
- Stack multiple policies for complex resource management

**RPC Layer Design**: Moved routing decisions to RPC boundary for:
- Cleaner separation between query processing and cluster selection
- Ability to apply routing logic before query execution begins
- Better testability with clear input/output contracts

### Data Flow
```
1. Query arrives at Snuba RPC endpoint
2. Routing strategy evaluates:
   - Request metadata (referrer, query type, organization)
   - Cluster load metrics from ClickHouse
   - Historical query outcomes
3. Allocation policies filter available clusters:
   - Check bytes scanned budgets
   - Verify concurrent query limits
   - Apply referrer-specific guardrails
4. Selected cluster receives query
5. Query execution and results logging (with correct metadata)
6. Outcomes feed back into routing strategy for future decisions
```

### Key Technical Decisions

**Why unify AllocationPolicy and RoutingStrategy?**
- Both needed dynamic configuration from operators
- Both needed serialization for API transport  
- Consolidating under `ConfigurableComponent` eliminated code duplication and created consistent configuration interface

**Why move to RPC layer?**
- Routing decisions need to happen before query parsing and optimization
- RPC layer has access to raw request metadata needed for routing
- Separation of concerns: routing logic doesn't belong in query execution pipeline

**Why namespace isolation for component registry?**
- Prevents name collisions between different component types
- Allows `BaseRoutingStrategy.all_names()` to only return routing strategies
- Makes component discovery more intuitive and less error-prone

**Why make allocation policies type-aware?**
- Select and delete queries have different resource profiles
- Policies need different enforcement for read vs. write operations
- Self-awareness eliminates need for external type tracking

### Technical Complexity Challenges Solved

1. **Metaclass Programming**: Extended `RegisteredClass` metaclass to support namespace isolation while maintaining backward compatibility
2. **Circular Dependencies**: Carefully ordered PR merges (#7379, #7375, then #7346) to avoid breaking builds
3. **Type System Complexity**: Maintained type safety across dynamic component instantiation using `TypeVar` and generic types
4. **API Design**: Created intuitive configuration API that balances flexibility with type safety
5. **UI State Management**: Built React components that handle async configuration updates with proper error handling

### Impact on System Performance

- **Query Routing**: Enables load balancing across multiple ClickHouse clusters based on real-time metrics
- **Resource Protection**: Prevents cluster overload through configurable allocation policies  
- **Operational Flexibility**: Operators can change routing behavior without code deployments
- **Observability**: Proper logging of routing decisions and referrers enables debugging and analysis
- **Extensibility**: New routing strategies can be added with minimal code changes

---

## Key Contributions

### 1. Infrastructure Foundation (PR #7143)
**Title:** feat(cbrs): get eap items cluster load  
**Impact:** Established foundational infrastructure for CBRS  
**Technical Details:**
- Built the mechanism to retrieve current ClickHouse cluster load metrics for EAP items
- Added 224 lines of new functionality across 3 files with 13 commits
- Demonstrates ability to build solid, foundational components

**SE2 Alignment:**
- ✅ Takes ownership of project infrastructure
- ✅ Builds high-quality, efficient tools

---

### 2. Major Architecture Refactoring (PR #7201)
**Title:** feat(cbrs): move routing strategies to rpc  
**Impact:** Significant architectural improvement moving routing logic to RPC layer  
**Technical Details:**
- Refactored 754 additions and 1,347 deletions across 30 files
- 20 commits showing iterative development
- Addressed 51 review comments, demonstrating collaboration and receptiveness to feedback
- Linked to Linear issue EAP-75

**SE2 Alignment:**
- ✅ Executes medium-complexity refactoring efficiently
- ✅ Makes thoughtful trade-offs in architectural decisions
- ✅ Collaborates effectively through code review process

---

### 3. Production Bug Fix (PR #7241)
**Title:** fix(cbrs): set request in routing decision  
**Impact:** Fixed production issue causing errors  
**Technical Details:**
- Small, targeted fix (12 additions, 4 deletions across 2 files)
- Addresses Sentry issue #6678390448
- Quick turnaround from identification to fix (June 15 - June 23)

**SE2 Alignment:**
- ✅ Demonstrates awareness of production risks
- ✅ Makes minimal, surgical changes to fix issues
- ✅ Shows good judgment in escalation (created PR for tracking)

---

### 4. Architecture Consolidation (PR #7337)
**Title:** feat(cbrs): unify allocation policy and routing strategy  
**Impact:** Improved system design by unifying two related concepts  
**Technical Details:**
- 1,006 additions and 425 deletions across 20 files
- Consolidated `AllocationPolicy` and `RoutingStrategy` under `ConfigurableComponent` abstraction
- Linked to Linear issue EAP-76
- Sets up cleaner frontend integration

**SE2 Alignment:**
- ✅ Proposes and implements insightful improvements to existing code
- ✅ Demonstrates understanding of "bigger picture" system design
- ✅ Identifies opportunities for cross-layer improvements (backend → frontend)

---

### 5. Backend API Development (PR #7346)
**Title:** feat(cbrs): Snuba Admin endpoints  
**Impact:** Built backend APIs for configuration management  
**Technical Details:**
- 632 additions and 197 deletions across 12 files
- Created endpoints for retrieving and configuring routing strategies
- Coordinated dependencies with PRs #7379 and #7375
- 47 review comments addressed

**SE2 Alignment:**
- ✅ Builds high-quality, well-structured APIs
- ✅ Demonstrates awareness of dependencies and project coordination
- ✅ Shows effective collaboration through extensive code review

---

### 6. Refactoring for Clarity (PR #7375)
**Title:** ref(cbrs): integrate resource identifier and policy type into AllocationPolicy  
**Impact:** Improved code organization and clarity  
**Technical Details:**
- 178 additions and 115 deletions across 17 files
- Removed ambiguous `storage_key` property in favor of clearer resource identifiers
- Made allocation policies self-aware of their type (select vs. delete)

**SE2 Alignment:**
- ✅ Makes code easier to use and maintain
- ✅ Shows attention to code quality and developer experience

---

### 7. Class Registry Improvement (PR #7379)
**Title:** ref(cbrs): introduce RegisteredClass into ConfigurableComponent  
**Impact:** Enhanced component discovery and instantiation system  
**Technical Details:**
- 129 additions and 235 deletions across 9 files (net reduction!)
- Improved API for retrieving components by namespace and class name
- Thoughtful design considering multiple approaches (documented in PR description)

**SE2 Alignment:**
- ✅ Demonstrates sound engineering judgment
- ✅ Documents trade-offs in uncertain situations
- ✅ Writes efficient code that removes unnecessary complexity

---

### 8. Full-Stack Feature Development (PR #7399)
**Title:** feat(cbrs): Snuba Admin UI (webpage)  
**Impact:** Delivered end-to-end user interface for CBRS configuration  
**Technical Details:**
- 438 additions and 294 deletions across 22 files
- 22 commits showing iterative UI development
- Includes screenshot demonstrating working UI
- 16 review comments addressed

**SE2 Alignment:**
- ✅ Delivers complete, user-facing features
- ✅ Builds tools that are solid and easy to use
- ✅ Growing skillset by working across the stack (backend + frontend)

---

### 9. Bug Fix - Registry Namespace Isolation (PR #7401)
**Title:** fix(cbrs): divide subclasses according to their namespaces  
**Impact:** Fixed class registry bug causing namespace pollution  
**Technical Details:**
- Identified issue where `BaseRoutingStrategy.all_names()` incorrectly returned allocation policies
- Small, focused fix (43 additions across 2 files)
- Clear problem description and solution in PR

**SE2 Alignment:**
- ✅ Identifies and fixes bugs proactively
- ✅ Makes minimal, surgical changes
- ✅ Communicates technical issues clearly

---

### 10. Architecture Migration (PR #7411)
**Title:** ref(cbrs): move eap allocation policies into the routing strategy layer  
**Impact:** Major refactoring to improve system architecture  
**Technical Details:**
- 636 additions and 269 deletions across 14 files
- Linked to Linear issue EAP-79
- Long-lived PR (Sept 17 - Oct 16) showing sustained ownership
- 49 review comments addressed

**SE2 Alignment:**
- ✅ Owns medium-complexity projects and sees them through
- ✅ Persists through extensive review cycles
- ✅ Demonstrates cross-team collaboration

---

### 11. Code Quality Improvement (PR #7421)
**Title:** ref(cbrs): no more default routing decision  
**Impact:** Simplified API by removing unnecessary parameter  
**Technical Details:**
- Small but impactful refactoring (3 additions, 38 deletions)
- Removed confusing default parameter pattern
- Quick turnaround (same day merge)

**SE2 Alignment:**
- ✅ Proposes practical improvements to existing code
- ✅ Makes code easier to understand and use
- ✅ Executes efficiently on small improvements

---

### 12. Production Data Fix (PR #7473)
**Title:** fix(cbrs): fix querylog referrer field  
**Impact:** Corrected logging data for better observability  
**Technical Details:**
- Minimal change (2 additions, 2 deletions across 2 files)
- Fixed referrer field that was incorrectly set to routing strategy name
- Improves monitoring and debugging capabilities

**SE2 Alignment:**
- ✅ Demonstrates awareness of operational concerns
- ✅ Makes thoughtful trade-offs (data correctness vs. minimal code change)

---

## Summary Statistics

- **Total PRs:** 12
- **Total Additions:** ~5,000+ lines
- **Total Deletions:** ~3,000+ lines (net positive impact with code cleanup)
- **Files Changed:** 100+ files across all PRs
- **Review Comments Addressed:** 180+ comments
- **Time Span:** April 2025 - October 2025 (7 months)
- **Project Complexity:** Medium (CBRS system spanning multiple components)

---

## SE2 Competencies Demonstrated

### Core Requirements
1. **Takes on ownership of small-complexity projects and sees them through efficiently**
   - ✅ Owned CBRS implementation end-to-end over 7 months
   - ✅ Coordinated multiple dependent PRs (#7346, #7379, #7375)
   - ✅ Followed through from initial infrastructure to UI delivery

2. **Demonstrates good awareness of goals, risks, and the "bigger picture"**
   - ✅ Made architectural decisions considering frontend implications (PR #7337)
   - ✅ Fixed production issues promptly (PR #7241, #7473)
   - ✅ Documented trade-offs in PR descriptions (PR #7379)
   - ✅ Linked work to Linear issues showing alignment with team goals

3. **Builds high quality software and tools—solid, easy to use, efficient**
   - ✅ Extensive code review engagement (180+ comments)
   - ✅ Refactored code for clarity (PRs #7375, #7379, #7421)
   - ✅ Delivered working UI with screenshots (PR #7399)
   - ✅ Made surgical fixes avoiding over-engineering (PRs #7241, #7401, #7473)

### Additional Expectations
1. **Understands the basics of product areas of Sentry related to the one they work on**
   - ✅ Worked across EAP, CBRS, and core Snuba infrastructure
   - ✅ Understood ClickHouse cluster management implications

2. **Identifies opportunities for simple cross-team collaborations**
   - ✅ Built APIs and UI for configuration management (enabling operations team)
   - ✅ Connected backend changes to frontend needs

3. **Growing their engineering skillset on a monthly basis**
   - ✅ Progressed from infrastructure (April) → architecture (June-Aug) → full-stack (Sept)
   - ✅ Worked with RPC, UI, ClickHouse, and configuration systems

---

## SE3 Growth Indicators

While excelling as an SE2, this work also shows emerging SE3 capabilities:

1. **Medium-complexity project ownership** - CBRS spans multiple components and lasted 7 months
2. **Insightful architectural improvements** - Unified allocation policies and routing strategies (PR #7337)
3. **Business context awareness** - Fixed production issues affecting customers (PR #7241)
4. **Cross-functional collaboration** - Worked with PM, design (UI), and infrastructure teams

---

## Key Achievements

1. **Delivered complete CBRS system** from infrastructure to UI over 7 months
2. **Made major architectural improvements** unifying configuration systems
3. **Maintained production quality** with quick bug fixes and careful review process
4. **Demonstrated full-stack capabilities** building both backend APIs and frontend UI
5. **Showed strong judgment** making surgical fixes rather than over-engineering
6. **Collaborated effectively** engaging with 180+ review comments constructively

This body of work demonstrates strong SE2 performance with clear progression toward SE3 capabilities.

# Dashboard Structure Analysis

## Summary by Type

### 1. EMAIL Dashboard (ID: 3)
- **Total Cards:** 14
- **Total Tabs:** 4
  - OneTime (id: 100)
  - Delta (id: 118)
  - Tab 3 (id: 101)
  - . (id: 117) - Users tab
- **Display Types:** pie, scalar, table, row
- **Cards with Click Behavior:** 12
- **Parameters (Filters):**
  - userId (string)
  - Delta (boolean)

### 2. MESSAGE Dashboard (ID: 1354)
- **Total Cards:** 11
- **Total Tabs:** 2
  - Tab 1 (id: 119)
  - . (id: 120) - Users tab
- **Display Types:** pie, scalar, table
- **Cards with Click Behavior:** 5
- **Parameters (Filters):**
  - UserId (string)
  - DMs (boolean)
  - Delta (boolean)

### 3. CONTENT Dashboard (ID: 2)
- **Total Cards:** 7
- **Total Tabs:** 2
  - Tab 1 (id: 145)
  - . (id: 146) - Users tab
- **Display Types:** pie, scalar, table
- **Cards with Click Behavior:** 5
- **Parameters (Filters):**
  - userId (string)
  - From Cloud Name (string)
  - To Cloud Name (string)
  - Delta (boolean)
  - email (string)

---

## Common Structure Across All Dashboards

### Card Display Types:
1. **scalar** - Single number display (e.g., Total Jobs: 63)
2. **pie** - Pie chart showing distribution
3. **table** - Data table with rows and columns
4. **row** - Horizontal bar chart (email only)

### Click Behaviors:
- All click behaviors link to OTHER dashboards (type: "link", linkType: "dashboard")
- They pass parameters from current card columns to target dashboard filters
- Example: Clicking a pie slice passes the status value to filter the detail dashboard

### Parameters (Filters):
- All dashboards have a **userId** filter
- Most have a **Delta** boolean filter
- Content has additional: From Cloud Name, To Cloud Name, email

### Tabs:
- All dashboards have a "." tab which contains the Users List
- Main data is on Tab 1 (or OneTime/Delta for email)

---

## What We Need to Build for Merged Dashboard Viewer

### 1. Tab Navigation
- Render tabs at top
- Switch between tabs to show different card groups
- Handle different tab structures per dashboard type

### 2. Card Types to Render

#### Scalar Cards
- Display: Large centered number
- Aggregation: SUM values from all sources
- Data structure: `rows: [[label, value]]` - value is in last column

#### Pie Charts
- Display: Actual pie chart with legend
- Aggregation: Combine all rows, group by label, sum values
- Data structure: `rows: [[label, value], [label, value], ...]`
- Need: Chart.js for real pie rendering

#### Tables
- Display: Scrollable table with headers
- Aggregation: UNION all rows from sources
- Data structure: `cols: [{name, display_name}], rows: [[...], [...]]`

#### Row/Bar Charts
- Display: Horizontal bars
- Aggregation: Combine rows, group by label, sum values
- Data structure: Same as pie

### 3. Filters
- Render filter dropdowns based on dashboard parameters
- When filter changes, re-fetch data with filter applied
- Common filters: userId, Delta

### 4. Layout
- Use CSS Grid with 24 columns (Metabase standard)
- Position cards based on: col, row, size_x, size_y
- Cards should respect their original positions

### 5. Interactivity (Limited)
- Click behaviors that link to Metabase dashboards WON'T work (different databases)
- We CAN show drill-down within our merged data (e.g., click pie slice to see breakdown by source)

---

## Implementation Plan

1. **Add Chart.js** for real chart rendering
2. **Update backend** to return complete structure including parameters
3. **Build filter UI** based on parameters
4. **Implement proper chart rendering** (pie, bar)
5. **Add drill-down** - click to see source breakdown
6. **Improve layout** - proper grid positioning

#!/usr/bin/env node

/**
 * Simple helper to extract values from context.json using dot notation
 * Usage: node bin/get-context.js ECR.repositoryName
 */

const fs = require('fs');
const path = require('path');

try {
  // Read context.json
  const contextPath = path.join(__dirname, '..', 'context', 'context.json');
  const context = JSON.parse(fs.readFileSync(contextPath, 'utf8'));
  
  // Get the path argument (e.g., "ECR.repositoryName")
  const dotPath = process.argv[2];
  
  if (!dotPath) {
    console.error('Error: No path specified. Usage: node bin/get-context.js ECR.repositoryName');
    process.exit(1);
  }
  
  // Traverse the object using dot notation
  const value = dotPath.split('.').reduce((obj, key) => {
    return obj && obj[key] !== undefined ? obj[key] : null;
  }, context);
  
  // Output the value (or empty string if not found)
  console.log(value !== null ? value : '');
  
} catch (error) {
  // Silent failure - output empty string so bash scripts can use fallback
  console.log('');
  process.exit(0);
}

import * as fs from 'fs';
import * as path from 'path';
import { minify } from 'html-minifier-terser';

/**
 * Build script to generate TemplateConstants.ts from mustache template files
 * Uses html-minifier-terser for professional template minification
 */

interface MinificationResult {
  original: string;
  minified: string;
  compressionRatio: number;
}

interface TemplateMetadata {
  generatedAt: string;
  minified: boolean;
  originalSize: {
    dashboard: number;
    login: number;
    partials: Record<string, number>;
    css: Record<string, number>;
    js: Record<string, number>;
  };
  minifiedSize: {
    dashboard: number;
    login: number;
    partials: Record<string, number>;
    css: Record<string, number>;
    js: Record<string, number>;
  };
}

/**
 * Minify CSS content
 */
function minifyCSS(content: string): MinificationResult {
  const original = content;
  const minified = original
    .replace(/\/\*[\s\S]*?\*\//g, '') // Remove CSS comments
    .replace(/\s+/g, ' ') // Collapse whitespace
    .replace(/;\s*}/g, '}') // Remove semicolon before closing brace
    .replace(/\s*{\s*/g, '{') // Remove spaces around opening braces
    .replace(/;\s*/g, ';') // Remove spaces after semicolons
    .trim();
  
  const compressionRatio = ((original.length - minified.length) / original.length * 100);
  return { original, minified, compressionRatio };
}

/**
 * Minify JavaScript content
 */
function minifyJS(content: string): MinificationResult {
  const original = content;
  const minified = original
    .replace(/\/\*[\s\S]*?\*\//g, '') // Remove multi-line comments
    .replace(/\/\/.*$/gm, '') // Remove single-line comments
    .replace(/\s+/g, ' ') // Collapse whitespace
    .replace(/;\s*}/g, '}') // Clean up before closing braces
    .replace(/\s*{\s*/g, '{') // Clean up around opening braces
    .trim();
  
  const compressionRatio = ((original.length - minified.length) / original.length * 100);
  return { original, minified, compressionRatio };
}

/**
 * Read and process static assets (CSS/JS) from public directory
 */
function readStaticAssets(publicDir: string): {
  css: Record<string, MinificationResult>;
  js: Record<string, MinificationResult>;
} {
  const css: Record<string, MinificationResult> = {};
  const js: Record<string, MinificationResult> = {};

  // Read CSS files
  const cssDir = path.join(publicDir, 'css');
  if (fs.existsSync(cssDir)) {
    const cssFiles = fs.readdirSync(cssDir).filter(file => file.endsWith('.css'));
    for (const file of cssFiles) {
      const cssPath = path.join(cssDir, file);
      const cssName = file.replace('.css', '');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');
      css[cssName] = minifyCSS(cssContent);
    }
  }

  // Read JS files
  const jsDir = path.join(publicDir, 'js');
  if (fs.existsSync(jsDir)) {
    const jsFiles = fs.readdirSync(jsDir).filter(file => file.endsWith('.js'));
    for (const file of jsFiles) {
      const jsPath = path.join(jsDir, file);
      const jsName = file.replace('.js', '');
      const jsContent = fs.readFileSync(jsPath, 'utf-8');
      js[jsName] = minifyJS(jsContent);
    }
  }

  return { css, js };
}

/**
 * HTML minification configuration optimized for Mustache templates
 */
const minificationOptions = {
  collapseWhitespace: true,
  removeComments: true,
  removeEmptyAttributes: true,
  removeRedundantAttributes: true,
  removeScriptTypeAttributes: true,
  removeStyleLinkTypeAttributes: true,
  /**
   * Disabled CSS minification to prevent interference with Mustache expressions in 
   * <style> tags. The css minifier in the HTMLMinifier-terser package does not
   * recognize template syntax and may corrupt the content. The JS minification
   * is safe to use as it does not affect template expressions.
   */
  minifyCSS: false,
  minifyJS: true,
  // Preserve mustache expressions and important whitespace
  ignoreCustomFragments: [
    /\{\{[\s\S]*?\}\}/,  // Mustache expressions
    /\{\{\{[\s\S]*?\}\}\}/,  // Mustache unescaped expressions
    /\{\{#[\s\S]*?\}\}/,  // Mustache sections
    /\{\{\/[\s\S]*?\}\}/,  // Mustache end sections
    /\{\{\^[\s\S]*?\}\}/,  // Mustache inverted sections
    /\{\{>[\s\S]*?\}\}/   // Mustache partials
  ],
  preserveLineBreaks: false,
  conservativeCollapse: true
};

/**
 * Minify template content using html-minifier-terser
 */
async function minifyTemplate(content: string): Promise<MinificationResult> {
  const original = content;
  const minified = await minify(original, minificationOptions);
  const compressionRatio = ((original.length - minified.length) / original.length * 100);
  
  return {
    original,
    minified,
    compressionRatio
  };
}

/**
 * Escape string for TypeScript template literal
 */
function escapeForTypeScript(str: string): string {
  return str
    .replace(/\\/g, '\\\\')
    .replace(/`/g, '\\`')
    .replace(/\${/g, '\\${');
}

/**
 * Generate TemplateConstants.ts file
 */
async function generateTemplateConstants(): Promise<void> {
  console.log('🔨 Generating template constants with html-minifier-terser...');

  const templatesDir = path.join(__dirname, '../templates');
  const partialsDir = path.join(templatesDir, 'partials');
  const publicDir = path.join(__dirname, '../public');
  const outputPath = path.join(__dirname, '../src/handlers/TemplateConstants.ts');

  // Read and minify main dashboard template
  const dashboardPath = path.join(templatesDir, 'dashboard.mustache');
  const dashboardOriginal = fs.readFileSync(dashboardPath, 'utf-8');
  const dashboardResult = await minifyTemplate(dashboardOriginal);

  // Read and minify main login template
  const loginPath = path.join(templatesDir, 'login.mustache');
  const loginOriginal = fs.readFileSync(loginPath, 'utf-8');
  const loginResult = await minifyTemplate(loginOriginal);

  // Read and minify partials
  const partials: Record<string, MinificationResult> = {};
  const partialFiles = ['individual-tab.mustache', 'mapping-tab.mustache', 'bulk-tab.mustache', 'history-tab.mustache', 'system-tab.mustache'];
  
  for (const file of partialFiles) {
    const partialPath = path.join(partialsDir, file);
    const partialName = file.replace('.mustache', '');
    if (fs.existsSync(partialPath)) {
      const partialOriginal = fs.readFileSync(partialPath, 'utf-8');
      partials[partialName] = await minifyTemplate(partialOriginal);
    }
  }

  // Read and process static assets
  const staticAssets = readStaticAssets(publicDir);

  // Add CSS files as partials to avoid Mustache parsing issues
  for (const [name, result] of Object.entries(staticAssets.css)) {
    partials[`${name}-styles`] = result;
  }

  // Create metadata
  const metadata: TemplateMetadata = {
    generatedAt: new Date().toISOString(),
    minified: true,
    originalSize: {
      dashboard: dashboardOriginal.length,
      login: loginOriginal.length,
      partials: Object.fromEntries(
        Object.entries(partials).map(([name, result]) => [name, result.original.length])
      ),
      css: Object.fromEntries(
        Object.entries(staticAssets.css).map(([name, result]) => [name, result.original.length])
      ),
      js: Object.fromEntries(
        Object.entries(staticAssets.js).map(([name, result]) => [name, result.original.length])
      )
    },
    minifiedSize: {
      dashboard: dashboardResult.minified.length,
      login: loginResult.minified.length,
      partials: Object.fromEntries(
        Object.entries(partials).map(([name, result]) => [name, result.minified.length])
      ),
      css: Object.fromEntries(
        Object.entries(staticAssets.css).map(([name, result]) => [name, result.minified.length])
      ),
      js: Object.fromEntries(
        Object.entries(staticAssets.js).map(([name, result]) => [name, result.minified.length])
      )
    }
  };

  // Generate TypeScript constants file
  const tsContent = `/**
 * Auto-generated template constants for Lambda deployment
 * Generated by BuildTemplates.ts at ${metadata.generatedAt}
 * Minified using html-minifier-terser
 * 
 * DO NOT EDIT THIS FILE MANUALLY - IT WILL BE OVERWRITTEN
 */

export const DASHBOARD_TEMPLATE = \`${escapeForTypeScript(dashboardResult.minified)}\`;

export const LOGIN_TEMPLATE = \`${escapeForTypeScript(loginResult.minified)}\`;

export const TEMPLATE_PARTIALS: Record<string, string> = {${
    Object.entries(partials)
      .map(([name, result]) => `\n  '${name}': \`${escapeForTypeScript(result.minified)}\``)
      .join(',')
  }
};

export const CSS_ASSETS: Record<string, string> = {${
    Object.entries(staticAssets.css)
      .map(([name, result]) => `\n  '${name}': \`${escapeForTypeScript(result.minified)}\``)
      .join(',')
  }
};

export const JS_ASSETS: Record<string, string> = {${
    Object.entries(staticAssets.js)
      .map(([name, result]) => `\n  '${name}': \`${escapeForTypeScript(result.minified)}\``)
      .join(',')
  }
};

// Template metadata
export const TEMPLATE_METADATA: TemplateMetadata = ${JSON.stringify(metadata, null, 2)};

export interface TemplateMetadata {
  generatedAt: string;
  minified: boolean;
  originalSize: {
    dashboard: number;
    login: number;
    partials: Record<string, number>;
    css: Record<string, number>;
    js: Record<string, number>;
  };
  minifiedSize: {
    dashboard: number;
    login: number;
    partials: Record<string, number>;
    css: Record<string, number>;
    js: Record<string, number>;
  };
}
`;

  // Write the constants file
  fs.writeFileSync(outputPath, tsContent);

  // Log compression statistics
  console.log('✅ Template constants generated successfully!');
  console.log(`📄 Dashboard template: ${dashboardOriginal.length} → ${dashboardResult.minified.length} bytes (${dashboardResult.compressionRatio.toFixed(1)}% reduction)`);
  console.log(`📄 Login template: ${loginOriginal.length} → ${loginResult.minified.length} bytes (${loginResult.compressionRatio.toFixed(1)}% reduction)`);
  
  const totalOriginalPartials = Object.values(partials).reduce((sum, result) => sum + result.original.length, 0);
  const totalMinifiedPartials = Object.values(partials).reduce((sum, result) => sum + result.minified.length, 0);
  const partialsCompression = ((totalOriginalPartials - totalMinifiedPartials) / totalOriginalPartials * 100);
  
  console.log(`📁 Partials: ${Object.keys(partials).length} files, ${totalOriginalPartials} → ${totalMinifiedPartials} bytes (${partialsCompression.toFixed(1)}% reduction)`);
  
  const totalOriginalCSS = Object.values(staticAssets.css).reduce((sum, result) => sum + result.original.length, 0);
  const totalMinifiedCSS = Object.values(staticAssets.css).reduce((sum, result) => sum + result.minified.length, 0);
  const cssCompression = totalOriginalCSS > 0 ? ((totalOriginalCSS - totalMinifiedCSS) / totalOriginalCSS * 100) : 0;
  
  const totalOriginalJS = Object.values(staticAssets.js).reduce((sum, result) => sum + result.original.length, 0);
  const totalMinifiedJS = Object.values(staticAssets.js).reduce((sum, result) => sum + result.minified.length, 0);
  const jsCompression = totalOriginalJS > 0 ? ((totalOriginalJS - totalMinifiedJS) / totalOriginalJS * 100) : 0;
  
  if (Object.keys(staticAssets.css).length > 0) {
    console.log(`🎨 CSS: ${Object.keys(staticAssets.css).length} files, ${totalOriginalCSS} → ${totalMinifiedCSS} bytes (${cssCompression.toFixed(1)}% reduction)`);
  }
  if (Object.keys(staticAssets.js).length > 0) {
    console.log(`⚡ JavaScript: ${Object.keys(staticAssets.js).length} files, ${totalOriginalJS} → ${totalMinifiedJS} bytes (${jsCompression.toFixed(1)}% reduction)`);
  }
  
  console.log(`💾 Output: ${outputPath}`);
  console.log('📝 Note: CSS/JS assets are now inlined - no external requests needed!');
  console.log('');
}

// Run if called directly
if (require.main === module) {
  generateTemplateConstants().catch(console.error);
}

export { generateTemplateConstants };
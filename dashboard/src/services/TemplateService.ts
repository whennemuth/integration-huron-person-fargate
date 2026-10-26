import { readFileSync } from 'fs';
import { join } from 'path';
import { isRunningInLambda } from '../Utils';

/**
 * Template service that provides templates from filesystem (local development) 
 * or embedded constants (Lambda deployment)
 */
export class TemplateService {

  /**
   * Get the main dashboard template
   */
  static getDashboardTemplate(): string {
    if (isRunningInLambda()) {
      return require('../handlers/TemplateConstants').DASHBOARD_TEMPLATE;
    } else {
      const templatePath = join(__dirname, '../../templates/dashboard.mustache');
      return readFileSync(templatePath, 'utf-8');
    }
  }

  /**
   * Get the login page template
   */
  static getLoginTemplate(): string {
    if (isRunningInLambda()) {
      return require('../handlers/TemplateConstants').LOGIN_TEMPLATE;
    } else {
      const templatePath = join(__dirname, '../../templates/login.mustache');
      return readFileSync(templatePath, 'utf-8');
    }
  }

  /**
   * Get all template partials
   */
  static getPartials(): Record<string, string> {
    if (isRunningInLambda()) {
      return require('../handlers/TemplateConstants').TEMPLATE_PARTIALS;
    } else {
      const partialsPath = join(__dirname, '../../templates/partials');
      return {
        'individual-tab': readFileSync(join(partialsPath, 'individual-tab.mustache'), 'utf-8'),
        'mapping-tab': readFileSync(join(partialsPath, 'mapping-tab.mustache'), 'utf-8'),
        'bulk-tab': readFileSync(join(partialsPath, 'bulk-tab.mustache'), 'utf-8'),
        'history-tab': readFileSync(join(partialsPath, 'history-tab.mustache'), 'utf-8'),
        'system-tab': readFileSync(join(partialsPath, 'system-tab.mustache'), 'utf-8')
      };
    }
  }

  /**
   * Get CSS assets for inlining in templates
   */
  static getCssAssets(): Record<string, string> {
    if (isRunningInLambda()) {
      return require('../handlers/TemplateConstants').CSS_ASSETS;
    } else {
      const cssPath = join(__dirname, '../../public/css');
      return {
        'dashboard': readFileSync(join(cssPath, 'dashboard.css'), 'utf-8')
      };
    }
  }

  /**
   * Get JavaScript assets for inlining in templates
   */
  static getJsAssets(): Record<string, string> {
    if (isRunningInLambda()) {
      const assets = require('../handlers/TemplateConstants').JS_ASSETS;
      // Convert hyphenated keys to camelCase for template compatibility
      return {
        'dashboard': assets['dashboard'] || '',
        'personCard': assets['person-card'] || '',
        'mappingExperimentCard': assets['mapping-experiment-card'] || ''
      };
    } else {
      const jsPath = join(__dirname, '../../public/js');
      return {
        'dashboard': readFileSync(join(jsPath, 'dashboard.js'), 'utf-8'),
        'personCard': readFileSync(join(jsPath, 'person-card.js'), 'utf-8'),
        'mappingExperimentCard': readFileSync(join(jsPath, 'mapping-experiment-card.js'), 'utf-8')
      };
    }
  }
}
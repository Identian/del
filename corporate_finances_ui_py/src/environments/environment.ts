// This file can be replaced during build by using the `fileReplacements` array.
// `ng build` replaces `environment.ts` with `environment.prod.ts`.
// The list of file replacements can be found in `angular.json`.

export const environment = {
  production: false,
  apiKey: 'https://ffkac914z7.execute-api.us-east-1.amazonaws.com/dev/',
  azure: {
    cliendId: 'dcd76a6c-30fe-4390-bf3c-dfefd29d304c',
    authority: 'https://login.microsoftonline.com/caa1dfbf-34d5-4061-9cdf-0ceaa516bf03'
  },
  KEY_ID_ASSESSMENT: "idAssessment",
  KEY_TOKEN: "token",
  KEY_COMPANIE_INFORMATION: "compaInformation"
};

/*
 * For easier debugging in development mode, you can import the following file
 * to ignore zone related error stack frames such as `zone.run`, `zoneDelegate.invokeTask`.
 *
 * This import should be commented out in production mode because it will have a negative impact
 * on performance if an error is thrown.
 */
// import 'zone.js/plugins/zone-error';  // Included with Angular CLI.
